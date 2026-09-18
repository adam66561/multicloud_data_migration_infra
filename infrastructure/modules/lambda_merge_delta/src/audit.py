# audit.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging

import boto3

logger = logging.getLogger(__name__)

def append_merge_audit(
    audit_table_name: str,
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
    dynamodb = boto3.resource("dynamodb")
    audit_table = dynamodb.Table(audit_table_name)
    now = datetime.now(timezone.utc)
    audit_item = {
        "file_id": f"{source_bucket}/{source_key}",
        "processed_at": now.isoformat(),
        "processed_date": now.date().isoformat(),
        "expires_at": int((now + timedelta(days=90)).timestamp()),
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
        "Audit write succeeded; source=s3://%s/%s",
        source_bucket,
        source_key,
    )
