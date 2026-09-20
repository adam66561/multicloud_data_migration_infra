# cdc.py
from __future__ import annotations

import json
from typing import TYPE_CHECKING

import boto3
import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from config import Settings

s3_client = boto3.client("s3")

def get_primary_keys(schema_name: str, table_name: str, settings: Settings) -> list[str]:
    response = s3_client.get_object(
        Bucket=settings.config_bucket,
        Key=settings.config_key,
    )
    config = json.loads(response["Body"].read().decode("utf-8"))
    table_key = f"{schema_name}.{table_name}".lower()
    primary_keys = config.get(table_key)

    if not primary_keys:
        raise RuntimeError(f"No primary key configured for {table_key}")

    normalized_primary_keys = [
        str(column).strip().lower()
        for column in primary_keys
    ]

    if not all(normalized_primary_keys):
        raise RuntimeError(
            f"Invalid primary key configuration for {table_key}: "
            "column names must not be empty"
        )
    
    return normalized_primary_keys  

def build_target_path(
    settings: object,
    target_prefix: str,
    table_name: str,
) -> str:
    return (
        f"s3://{settings.target_bucket}/"
        f"{target_prefix}/{table_name}/"
    )

def get_target_prefix_and_schema(source_prefix: str, prefix_config: dict[str, str]) -> tuple[str, str]:
    try:
        target_prefix = prefix_config[source_prefix.lower()].strip("/")
    except KeyError as exc:
        raise RuntimeError(
            f"No target prefix configured for schema/source prefix: {source_prefix}. "
            f"Configured values: {sorted(prefix_config.keys())}"
        ) from exc

    target_parts = [part for part in target_prefix.split("/") if part]

    if not target_parts:
        raise RuntimeError(
            f"Target prefix is empty for source prefix: {source_prefix}"
        )

    target_schema = target_parts[-1].lower()

    return target_prefix, target_schema

def parse_source_location(
    key: str,
    settings: Settings,
) -> tuple[str, str, str]:
    """
    Expected form:
        <optional-prefix>/<schema>/<table>/
        <0..N date partition folders>/
        <file.parquet>
    Returns:
        target_prefix, target_schema, source_table
    """
    key_parts = [part for part in key.strip("/").split("/") if part]

    if settings.date_partition_subfolder_count < 0:
        raise ValueError(
            "date_partition_subfolder_count must be zero or greater."
        )

    # schema + table + N date folders + file
    minimum_key_parts = 3 + settings.date_partition_subfolder_count

    if len(key_parts) < minimum_key_parts:
        raise RuntimeError(
            "CDC S3 key has fewer components than expected. "
            f"Expected at least {minimum_key_parts} components for "
            f"{settings.date_partition_subfolder_count} date partition subfolder(s). "
            f"Received: {key}"
        )

    table_index = -(settings.date_partition_subfolder_count + 2)

    source_table = key_parts[table_index].lower()
    source_prefix = "/".join(key_parts[:table_index])

    target_prefix, target_schema = get_target_prefix_and_schema(source_prefix, settings.prefix_mapping)

    return target_prefix, target_schema, source_table


def build_final_state(
    table: pa.Table,
    pk_cols: list[str],
) -> tuple[pa.Table | None, int]:
    if table.num_rows == 0:
        return table, 0
    
    groups: dict[tuple, list[int]] = {}

    for row_index in range(table.num_rows):
        pk = tuple(
            table[col][row_index].as_py()
            for col in pk_cols
        )

        groups.setdefault(pk, []).append(row_index)

    final_rows = []

    for indices in groups.values():
        group = table.take(
            pa.array(indices, type=pa.int64())
        )

        last_row = group.slice(
            group.num_rows - 1,
            1,
        )

        last_op = last_row["op"][0].as_py()

        # Last event is DELETE:
        # preserve the exact delete event.
        if last_op == "D":
            final_rows.append(last_row)
            continue

        # Find the latest DELETE.
        delete_mask = pc.equal(
            group["op"],
            pa.scalar("D", type=group["op"].type),
        )

        delete_indices = pc.indices_nonzero(
            delete_mask
        ).to_pylist()

        # DELETE resets entity state.
        # Only events after the most recent DELETE
        # belong to the current lifecycle.
        if delete_indices:
            last_delete_index = delete_indices[-1]

            group = group.slice(
                last_delete_index + 1
            )

        result_arrays = []

        for col in table.column_names:
            column_type = table.schema.field(col).type

            if col in pk_cols or col == "op":
                value = group[col][-1]

            else:
                non_null_values = pc.drop_null(
                    group[col]
                )

                if len(non_null_values) > 0:
                    value = non_null_values[-1]
                else:
                    value = pa.scalar(
                        None,
                        type=column_type,
                    )

            result_arrays.append(
                pa.array(
                    [value],
                    type=column_type,
                )
            )

        final_rows.append(
            pa.Table.from_arrays(
                result_arrays,
                names=table.column_names,
            )
        )

    final_table = pa.concat_tables(final_rows)

    return final_table, final_table.num_rows