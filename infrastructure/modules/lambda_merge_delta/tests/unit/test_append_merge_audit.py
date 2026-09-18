# test_append_merge_audit.py
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import sys
sys.path.insert(0, "../../src")
import audit


def test_append_merge_audit_writes_complete_audit_item():
    fixed_now = datetime(2026, 9, 18, 14, 0, 0, tzinfo=timezone.utc)

    metrics = {
        "num_source_rows": 100,
        "num_target_rows_inserted": 50,
        "num_target_rows_updated": 30,
        "num_target_rows_deleted": 20,
        "num_target_files_added": 4,
        "num_target_files_removed": 2,
        "execution_time_ms": 1_500,
        "scan_time_ms": 200,
        "rewrite_time_ms": 1_000,
    }

    mock_table = Mock()
    mock_dynamodb = Mock()
    mock_dynamodb.Table.return_value = mock_table

    with (
        patch.object(audit.boto3, "resource", return_value=mock_dynamodb),
        patch.object(audit, "datetime") as mock_datetime,
    ):
        mock_datetime.now.return_value = fixed_now

        audit.append_merge_audit(
            audit_table_name="merge-audit",
            source_bucket="raw-bucket",
            source_key="cdc/public/customers/file.parquet",
            schema_name="public",
            table_name="customers",
            target_path="s3://lakehouse/silver/public/customers",
            input_rows=100,
            final_state_rows=80,
            metrics=metrics,
            attempt=2,
        )

    mock_dynamodb.Table.assert_called_once_with("merge-audit")
    mock_table.put_item.assert_called_once()

    written_item = mock_table.put_item.call_args.kwargs["Item"]

    assert written_item == {
        "file_id": "raw-bucket/cdc/public/customers/file.parquet",
        "processed_at": "2026-09-18T14:00:00+00:00",
        "processed_date": "2026-09-18",
        "expires_at": int((fixed_now + timedelta(days=90)).timestamp()),
        "status": "SUCCEEDED",
        "source_bucket": "raw-bucket",
        "source_key": "cdc/public/customers/file.parquet",
        "schema_name": "public",
        "table_name": "customers",
        "target_path": "s3://lakehouse/silver/public/customers",
        "input_rows": 100,
        "final_state_rows": 80,
        "merge_attempt": 2,
        "num_source_rows": 100,
        "num_target_rows_inserted": 50,
        "num_target_rows_updated": 30,
        "num_target_rows_deleted": 20,
        "num_target_files_added": 4,
        "num_target_files_removed": 2,
        "execution_time_ms": 1500,
        "scan_time_ms": 200,
        "rewrite_time_ms": 1000,
    }