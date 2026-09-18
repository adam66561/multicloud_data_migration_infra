# cdc.py
from __future__ import annotations

import json
from typing import TYPE_CHECKING

import boto3
import pandas as pd

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
    df: pd.DataFrame,
    pk_cols: list[str],
) -> pd.DataFrame:
    payload_cols = [
        col
        for col in df.columns
        if col not in set(pk_cols + ["op"])
    ]
    final_rows = []

    for _, group in df.groupby(pk_cols, sort=False, dropna=False):
        final_row = group.iloc[-1].copy()

        # The last event is a delete: delete this PK from Delta.
        if final_row["op"] == "D":
            final_rows.append(final_row)
            continue

        # For I/U, retain the latest non-null changed value per column.
        for col in payload_cols:
            changed_values = group[col].dropna()

            final_row[col] = (
                changed_values.iloc[-1]
                if not changed_values.empty
                else pd.NA
            )

        final_rows.append(final_row)

    if not final_rows:
        return df.iloc[0:0].copy()
    result = pd.DataFrame(final_rows, columns=df.columns).reset_index(drop=True)

    return result.astype(df.dtypes.to_dict())