import boto3
import json
import logging
import math
import os
import random
import time

# import base64
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Literal
from io import BytesIO

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOTO3_SESSION = boto3.Session()
S3_CLIENT = BOTO3_SESSION.client("s3")

def delta_table_exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except Exception:
        return False

def load_primary_keys(schema_name: str, table_name: str) -> List[str]:
    response = S3_CLIENT.get_object(
        Bucket= os.environ["S3_CONFIG_BUCKET"],
        Key=os.environ["S3_CONFIG_KEY"],
    )
    config = json.loads(response["Body"].read().decode("utf-8"))
    table_key = f"{schema_name}.{table_name}".lower()
    primary_keys = config.get(table_key)
    if not primary_keys:
        raise RuntimeError(f"No primary key configured for {table_key}")
    return [str(column).lower() for column in primary_keys]


def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    response = S3_CLIENT.get_object(Bucket=bucket, Key=key,)
    df = pd.read_parquet(
        BytesIO(response["Body"].read()),
        engine="pyarrow",
    )
    df.columns = [str(column).lower() for column in df.columns]
    if "op" not in df.columns:
        raise RuntimeError("DMS Parquet file does not contain an op column")
    df["op"] = df["op"].astype(str).str.upper()
    return df

def validate_primary_keys(
    df: pd.DataFrame,
    pk_cols: List[str],
    s3_target_path: str,
) -> None:
    missing_columns = [
        column
        for column in pk_cols
        if column not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            f"DataFrame missing PK columns {missing_columns} "
            f"for {s3_target_path}"
        )

    null_pk_rows = df[df[pk_cols].isna().any(axis=1)]

    if not null_pk_rows.empty:
        raise RuntimeError(
            f"Found {len(null_pk_rows)} row(s) with null primary keys "
            f"for {s3_target_path}"
        )

def merge_once(
        df: pd.DataFrame, 
        operation: str,
        s3_target_path: str,
        pk_cols: List[str]) -> Tuple[DeltaTable, str]:

    dt = DeltaTable(s3_target_path)
    merge_predicate = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])

    if operation in ("I", "U"):
        source_table = pa.Table.from_pandas(df, preserve_index=False,)
        update_map = {
            column: f"COALESCE(source.`{column}`, target.`{column}`)"
            for column in df.columns
            if column not in pk_cols
        }
        insert_map = {
            column: f"source.`{column}`"
            for column in df.columns
        }
        (
            dt.merge(
                source=source_table,
                predicate=merge_predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(updates=update_map)
            .when_not_matched_insert(updates=insert_map)
            .execute()
        )
    elif operation in ("D"):
        source_table = pa.Table.from_pandas(df[pk_cols], preserve_index=False,)
        (
            dt.merge(
                source=source_table,
                predicate=merge_predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_delete()
            .execute()
        )

    else:
        raise ValueError(f"Unsupported operation: {operation}")

    return dt, merge_predicate


def merge_with_retry(
    df: pd.DataFrame,
    operation: str,
    s3_target_path: str,
    pk_cols: List[str],
    max_attempts: int = 3,
):
    validate_primary_keys(df, pk_cols, s3_target_path)

    last_exception: Optional[Exception] = None
    last_dt: Optional[DeltaTable] = None
    merge_predicate: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        try:
            last_dt, merge_predicate = merge_once(
                df=df,
                operation=operation,
                s3_target_path=s3_target_path,
                pk_cols=pk_cols,
            )

            logger.info(f"SUCCESS: {operation} completed for {s3_target_path}")
            return

        except Exception as e:
            last_exception = e

            if attempt == max_attempts:
                break

            sleep_seconds = min(8.0, (2 ** (attempt - 1)) + random.uniform(0.1, 0.8))
            logger.warning(
                f"Retryable {operation} failure on {s3_target_path}, "
                f"attempt {attempt}/{max_attempts}: {e}. "
                f"Sleeping {sleep_seconds:.2f}s before retry."
            )
            time.sleep(sleep_seconds)

    if last_dt is not None:
        logger.info(f"Delta schema: {[f.name for f in last_dt.schema().fields]}")

    logger.info(f"PK cols: {pk_cols}")
    logger.info(f"Merge predicate: {merge_predicate}")

    if last_exception is None:
        raise RuntimeError(f"Unknown failure during {operation} for {s3_target_path}")
    raise last_exception


def extract_schema_table_from_s3_key(key: str) -> Tuple[str, str]:
    parts = [
        part
        for part in key.strip("/").split("/")
        if part
    ]

    if len(parts) < 4:
        raise RuntimeError(
            "cdc S3 key must contain at least "
            "cdc/<schema>/<table>/<file.parquet>. "
            f"Received: {key}"
        )

    return parts[1].lower(), parts[2].lower()


# assuming that order in parquet file is preserved as real event happened
# there is no good candidate for sequence column (optime has the same value for rows sometimes)
def build_final_state(
    df: pd.DataFrame,
    pk_cols: List[str],
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    required_columns = set(pk_cols + ["op"])
    missing = required_columns - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in DataFrame: {sorted(missing)}")

    payload_cols = [
        col
        for col in df.columns
        if col not in set(pk_cols + ["op"])
    ]

    final_rows = []

    # sort=False preserves first-seen PK group order.
    # Each group itself retains its original DataFrame row order.
    for _, group in df.groupby(pk_cols, sort=False, dropna=False):
        final_row = group.iloc[-1].copy()

        # The last event is a delete: delete this PK from Delta.
        if final_row["op"] == "D":
            final_rows.append(final_row)
            continue

        # For I/U, retain the latest non-null changed value per column.
        for col in payload_cols:
            changed_values = group[col].dropna()

            if not changed_values.empty:
                final_row[col] = changed_values.iloc[-1]
            else:
                final_row[col] = None

        final_rows.append(final_row)

    return pd.DataFrame(final_rows).reset_index(drop=True)


def process_parquet(
    s3_source_bucket: str,
    s3_source_key: str,
    s3_target_bucket: str,
):
    schema_name, table_name = extract_schema_table_from_s3_key(s3_source_key)
    pk_cols = load_primary_keys(schema_name, table_name)
    table_key = f"{schema_name}.{table_name}"

    s3_target_path = (f"s3://{s3_target_bucket}/{schema_name}/{table_name}/")

    df = read_parquet(s3_source_bucket, s3_source_key)

    logger.info(
        f"Processing {len(df)} rows from "
        f"s3://{s3_source_bucket}/{s3_source_key} as {table_key}"
    )

    final_df = build_final_state(df, pk_cols)
    upserts_df = final_df[final_df["op"].isin(["I", "U"])].copy()
    deletes_df = final_df[final_df["op"] == "D"].copy()

    if not upserts_df.empty:
        merge_with_retry(
            df=upserts_df,
            operation="I",
            s3_target_path=s3_target_path,
            pk_cols=pk_cols,
            max_attempts=3,
        )
    if not deletes_df.empty:
        merge_with_retry(
            df=deletes_df,
            operation="D",
            s3_target_path=s3_target_path,
            pk_cols=pk_cols,
            max_attempts=3,
        )


def lambda_handler(event, context):
    try:
        s3_source_bucket = os.environ['S3_SOURCE_BUCKET']
        s3_target_bucket = os.environ['S3_TARGET_BUCKET']
    except KeyError as e:
        logger.error(f"Missing env variable: {e}")
        raise RuntimeError(f"Configuration error: {e}")

    records = event.get("Records", [])
    if not records:
        logger.info("No S3 records found in Lambda event")
        return {"statusCode": 200,"processed": 0}

    processed = 0
    for record in records:
        if record.get("eventSource") != "aws:s3":
            continue

        event_bucket = record["s3"]["bucket"]["name"]
        event_key = record["s3"]["object"]["key"]

        if event_bucket != s3_source_bucket:
            logger.warning(
                f"Skipping S3 object from unexpected bucket: {event_bucket}. "
                f"Expected: {s3_source_bucket}"
            )
            continue

        if not event_key.lower().endswith(".parquet"):
            continue

        process_parquet(
            s3_source_bucket=event_bucket,
            s3_source_key=event_key,
            s3_target_bucket=s3_target_bucket,
        )
        processed += 1


    return {'statusCode': 200, 'processed': processed}