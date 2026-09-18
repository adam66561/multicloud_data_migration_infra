# lambda_function.py

import json
import logging
from urllib.parse import unquote_plus

from config import load_settings
from cdc import build_final_state, build_target_path, get_primary_keys, parse_source_location
from delta_merge import delta_table_exists, merge_with_retry
from s3_io import read_parquet
from audit import append_merge_audit

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_parquet(
    source_bucket: str,
    source_key: str,
    settings: object
) -> None:
    target_prefix, schema_name, table_name = parse_source_location(key=source_key, settings=settings)
    primary_keys = get_primary_keys(schema_name, table_name, settings)
    target_path = build_target_path(settings, target_prefix, table_name)

    if not delta_table_exists(target_path):
        raise RuntimeError(
            "Delta table does not exist or is not readable as a Delta table: "
            f"{schema_name}.{table_name} at {target_path}"
        )

    df = read_parquet(source_bucket, source_key, primary_keys)

    if df is None or df.empty:
        logger.warning(
            "No data read from S3 object: "
            f"s3://{source_bucket}/{source_key}"
        )
        return

    final_df = build_final_state(df, primary_keys)

    metrics, attempt = merge_with_retry(
        df=final_df,
        target_path=target_path,
        pk_cols=primary_keys,
        max_attempts=3,
    )

    if settings.audit_logs:
        append_merge_audit(
            audit_table_name=settings.audit_table_name,
            source_bucket=source_bucket,
            source_key=source_key,
            schema_name=schema_name,
            table_name=table_name,
            target_path=target_path,
            input_rows=len(df),
            final_state_rows=len(final_df),
            metrics=metrics,
            attempt=attempt,
        )


def lambda_handler(event, context):
    settings = load_settings()

    records = event.get("Records", [])
    if not records:
        logger.warning("No records found in Lambda event")
        return {"statusCode": 200,"processed": 0}

    processed = 0
    for record in records:
        if record.get("eventSource") != "aws:sqs": 
            continue

        eventbridge_event = json.loads(record["body"])
        bucket = (eventbridge_event["detail"]["bucket"]["name"])
        key = (eventbridge_event["detail"]["object"]["key"])
        
        if bucket != settings.source_bucket:
            logger.warning(
                f"Skipping S3 object from unexpected bucket: {bucket}. "
                f"Expected: {settings.source_bucket}"
            )
            continue

        if not key.lower().endswith(".parquet"):
            logger.warning(
                f"Skipping S3 object with unsupported file extension: {key}. "
                f"Expected a .parquet file."
            )
            continue

        process_parquet(
            source_bucket=bucket,
            source_key=key,
            settings=settings
        )

        processed += 1

    return {'statusCode': 200, 'processed': processed}