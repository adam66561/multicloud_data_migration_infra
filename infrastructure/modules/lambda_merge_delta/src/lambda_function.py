import boto3
from botocore.exceptions import ClientError
import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from typing import List, Tuple
from io import BytesIO

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOTO3_SESSION = boto3.Session()
S3_CLIENT = BOTO3_SESSION.client("s3")
dynamodb = boto3.resource("dynamodb")

def delta_table_exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except Exception as exc:
        logger.warning(
            "Unable to load Delta table at %s: %s",
            path,
            exc,
        )
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


def read_parquet(bucket: str, key: str, pk_cols: List[str]) -> pd.DataFrame:
    try:
        response = S3_CLIENT.get_object(Bucket=bucket, Key=key,)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]

        if error_code in ("NoSuchKey", "404"):
            logger.warning(
                "CDC file no longer exists; skipping: s3://%s/%s",
                bucket,
                key,
            )
            return None

        logger.exception(
            "Unable to read CDC object: s3://%s/%s; code=%s",
            bucket,
            key,
            error_code,
        )
        raise

    df = pd.read_parquet(
        BytesIO(response["Body"].read()),
        engine="pyarrow",
    )

    df.columns = [str(column).lower() for column in df.columns]

    required_columns = set(pk_cols + ["op", "optime"])

    missing_columns = (required_columns - set(df.columns))

    if missing_columns:
        raise RuntimeError(
            f"Missing required columns in Parquet: "
            f"{sorted(missing_columns)}"
        )

    if df[pk_cols].isna().any(axis=1).any():
        raise RuntimeError(
            "Parquet contains rows with null primary keys"
        )

    df["op"] = df["op"].astype(str).str.upper()

    invalid_ops = (
        ~df["op"].isin(["I", "U", "D"])
    )

    if invalid_ops.any():
        raise RuntimeError(
            f"Unsupported operations found: "
            f"{df.loc[invalid_ops, 'op'].unique().tolist()}"
        )
    
    return df


def extract_schema_table_from_s3_key(key: str, cdc_path: str) -> Tuple[str, str]:
    key_parts = [ part for part in key.strip("/").split("/") if part ]
    prefix_parts = [ part for part in cdc_path.strip("/").split("/") if part ]

    if key_parts[:len(prefix_parts)] != prefix_parts:
        raise RuntimeError(
            f"S3 key does not start with configured CDC path. "
            f"key={key}, cdc_path={cdc_path}"
        )

    relative_parts = key_parts[len(prefix_parts):]

    if len(relative_parts) < 3:
        raise RuntimeError(
            "CDC S3 key must contain "
            "<cdc-prefix>/<schema>/<table>/<file.parquet>. "
            f"Received: {key}"
        )

    schema_name = relative_parts[0].lower()
    table_name = relative_parts[1].lower()

    return schema_name, table_name


def merge_once(
        df: pd.DataFrame, 
        s3_target_path: str,
        pk_cols: List[str]
    ) -> None:

    dt = DeltaTable(s3_target_path)

    merge_predicate = " AND ".join(
    [
        f"target.`{column}` = source.`{column}`"
        for column in pk_cols
    ]
    )

    source_table = pa.Table.from_pandas(df, preserve_index=False,)

    target_columns = {
        field.name
        for field in dt.schema().fields
    }

    new_columns = [
        column
        for column in df.columns
        if column not in target_columns
    ]

    logger.info(
        f"Starting merge for {s3_target_path}; "
        f"rows={len(df)}; "
        f"pk_cols={pk_cols}; "
        f"new_columns={new_columns}; "
        f"predicate={merge_predicate}"
    )

    update_map = {
        column: (
            f"COALESCE(source.`{column}`, target.`{column}`)"
            if column in target_columns
            else f"source.`{column}`"
        )
        for column in df.columns
        if column not in pk_cols
    }

    insert_map = {
        column: f"source.`{column}`"
        for column in df.columns
    }

    metrics = (
        dt.merge(
            source=source_table,
            predicate=merge_predicate,
            source_alias="source",
            target_alias="target",
            merge_schema=True,
        )
        .when_matched_delete(predicate=(
            "source.op = 'D' "
            "AND source.optime >= target.optime")
        )
        .when_matched_update(predicate=(
            "source.op IN ('I', 'U') "
            "AND source.optime >= target.optime"),
            updates=update_map
        )
        .when_not_matched_insert(predicate=(
            "source.op IN ('I', 'U')"), 
            updates=insert_map
        )
        .execute()
    )

    return metrics


def merge_with_retry(
    df: pd.DataFrame,
    s3_target_path: str,
    pk_cols: List[str],
    max_attempts: int = 3,
):
    for attempt in range(1, max_attempts + 1):
        try:
            metrics = merge_once(
                df=df,
                s3_target_path=s3_target_path,
                pk_cols=pk_cols,
            )

            logger.info(f"SUCCESS: completed for {s3_target_path}")
            return metrics, attempt

        except Exception as e:
            if attempt == max_attempts:
                logger.error(f"Merge failed after {max_attempts} attempts for {s3_target_path}: {e}")
                raise

            sleep_seconds = min(8.0, (2 ** (attempt - 1)) + random.uniform(0.1, 0.8))
            logger.warning(
                f"Retry on {s3_target_path}, "
                f"attempt {attempt}/{max_attempts}: {e}. "
                f"Sleeping {sleep_seconds:.2f}s before retry."
            )
            time.sleep(sleep_seconds)


# assuming that order in parquet file is preserved as real event happened
# there is no good candidate for sequence column (optime has the same value for rows sometimes)
def build_final_state(
    df: pd.DataFrame,
    pk_cols: List[str],
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

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


def append_merge_audit(
    audit_table: str,
    source_bucket: str,
    source_key: str,
    schema_name: str,
    table_name: str,
    target_path: str,
    input_rows: int,
    final_state_rows: int,
    metrics: dict,
    attempt: int,
) -> None:
    now = datetime.now(timezone.utc)
    audit_item = {
        "file_id": f"{source_bucket}/{source_key}",
        "processed_at": now.isoformat(),
        "processed_date": now.date().isoformat(),
        "status": "SUCCEEDED",
        "source_bucket": source_bucket,
        "source_key": source_key,
        "schema_name": schema_name,
        "table_name": table_name,
        "target_path": target_path,
        "input_rows": int(input_rows),
        "final_state_rows": int(final_state_rows),
        "merge_attempt": int(attempt),
        "num_source_rows": int(metrics.get("num_source_rows", 0)),
        "num_target_rows_inserted": int(metrics.get(
            "num_target_rows_inserted", 0
        )),
        "num_target_rows_updated": int(metrics.get(
            "num_target_rows_updated", 0
        )),
        "num_target_rows_deleted": int(metrics.get(
            "num_target_rows_deleted", 0
        )),
        "num_target_files_added": int(metrics.get(
            "num_target_files_added", 0
        )),
        "num_target_files_removed": int(metrics.get(
            "num_target_files_removed", 0
        )),
        "execution_time_ms": int(metrics.get("execution_time_ms", 0)),
        "scan_time_ms": int(metrics.get("scan_time_ms", 0)),
        "rewrite_time_ms": int(metrics.get("rewrite_time_ms", 0)),
    }

    audit_table.put_item(Item=audit_item)

    logger.info(
        "Audit write succeeded; source=s3://%s/%s; "
        "audit_attempt=%s",
        source_bucket,
        source_key,
    )


def process_parquet(
    s3_source_bucket: str,
    s3_source_key: str,
    s3_source_cdc_path: str,
    s3_target_bucket: str,
    s3_target_path: str,
    audit_logs: bool,
    audit_table: str,
):
    schema_name, table_name = extract_schema_table_from_s3_key(s3_source_key, s3_source_cdc_path)
    pk_cols = load_primary_keys(schema_name, table_name)
    table_key = f"{schema_name}.{table_name}"

    if s3_target_path == "":
        s3_target_path = (f"s3://{s3_target_bucket}/{schema_name}/{table_name}/")
    else:
        s3_target_path = (f"s3://{s3_target_bucket}/{s3_target_path}/{schema_name}/{table_name}/")

    if not delta_table_exists(s3_target_path):
        raise RuntimeError(
            f"Delta table not ready yet: "
            f"{schema_name}.{table_name}"
        )

    df = read_parquet(s3_source_bucket, s3_source_key, pk_cols)

    if df is None:
        return

    logger.info(
        f"Processing {len(df)} rows from "
        f"s3://{s3_source_bucket}/{s3_source_key} as {table_key}"
    )

    final_df = build_final_state(df, pk_cols)

    if final_df.empty:
        logger.info(
            "No final CDC state to merge; source=s3://%s/%s",
            s3_source_bucket,
            s3_source_key,
        )
        return

    metrics, attempt = merge_with_retry(
        df=final_df,
        s3_target_path=s3_target_path,
        pk_cols=pk_cols,
        max_attempts=3,
    )

    if audit_logs:
        append_merge_audit(
            audit_table=audit_table,
            source_bucket=s3_source_bucket,
            source_key=s3_source_key,
            schema_name=schema_name,
            table_name=table_name,
            target_path=s3_target_path,
            input_rows=len(df),
            final_state_rows=len(final_df),
            metrics=metrics,
            attempt=attempt,
        )


def lambda_handler(event, context):
    try:
        s3_source_bucket = os.environ['S3_SOURCE_BUCKET']
        s3_source_cdc_path = os.environ['S3_SOURCE_CDC_PATH']
        s3_target_bucket = os.environ['S3_TARGET_BUCKET']
        s3_target_path = os.environ.get('S3_TARGET_PATH', '')
        type_of_event = os.environ['EVENT_TYPE']
        audit_logs = os.environ.get('AUDIT_LOGS', 'false').lower() == 'true'
        audit_table = os.environ['AUDIT_TABLE_NAME']
    except KeyError as e:
        logger.error(f"Missing env variable: {e}")
        raise RuntimeError(f"Configuration error: {e}")

    records = event.get("Records", [])
    if not records:
        logger.info("No records found in Lambda event")
        return {"statusCode": 200,"processed": 0}

    processed = 0
    for record in records:
        if type_of_event == "fifo":
            if record.get("eventSource") != "aws:sqs": 
                continue
            eventbridge_event = json.loads(record["body"])
            event_bucket = (eventbridge_event["detail"]["bucket"]["name"])
            event_key = (eventbridge_event["detail"]["object"]["key"])
        elif type_of_event == "s3":
            if record.get("eventSource") != "aws:s3": 
                continue
            event_bucket = record["s3"]["bucket"]["name"]
            event_key = record["s3"]["object"]["key"]
        else:
            logger.error(f"Unsupported EVENT_TYPE: {type_of_event}")
            raise RuntimeError(f"Unsupported EVENT_TYPE: {type_of_event}")
        
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
            s3_source_cdc_path=s3_source_cdc_path,
            s3_target_bucket=s3_target_bucket,
            s3_target_path=s3_target_path,
            audit_logs=audit_logs,
            audit_table=audit_table
        )

        processed += 1

    return {'statusCode': 200, 'processed': processed}