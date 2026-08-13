import json
import logging
import os
import uuid
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

DEFAULT_ROWS_PER_FILE = 100


def write_parquet_to_s3(
    table: pa.Table,
    bucket: str,
    prefix: str,
) -> str:
    file_id = uuid.uuid4().hex
    filename = f"data-{file_id}.parquet"
    local_path = Path("/tmp") / filename

    pq.write_table(
        table,
        local_path,
        compression="snappy",
    )

    s3_key = f"{prefix.strip('/')}/{filename}"

    logger.info(
        "Uploading Parquet file: s3://%s/%s",
        bucket,
        s3_key,
    )

    try:
        s3.upload_file(
            str(local_path),
            bucket,
            s3_key,
        )
    finally:
        local_path.unlink(missing_ok=True)

    return f"s3://{bucket}/{s3_key}"


def split_records(
    records: list[dict],
    rows_per_file: int,
) -> list[list[dict]]:
    return [
        records[index:index + rows_per_file]
        for index in range(0, len(records), rows_per_file)
    ]


def process_subcatalog(
    bucket: str,
    config: dict,
) -> list[str]:
    path = config["path"].strip("/")
    records = config["records"]
    rows_per_file = config.get(
        "rows_per_file",
        DEFAULT_ROWS_PER_FILE,
    )

    generated_files = []

    for record_batch in split_records(records, rows_per_file):
        table = pa.Table.from_pylist(record_batch)

        s3_uri = write_parquet_to_s3(
            table=table,
            bucket=bucket,
            prefix=path,
        )

        generated_files.append(s3_uri)

    return generated_files


def validate_event(event: dict) -> None:
    if not isinstance(event, dict):
        raise ValueError("Event must be an object.")

    if "subcatalogs" not in event:
        raise ValueError("Event must contain 'subcatalogs'.")

    if not isinstance(event["subcatalogs"], list):
        raise ValueError("'subcatalogs' must be a list.")

    if not event["subcatalogs"]:
        raise ValueError("'subcatalogs' cannot be empty.")

    for index, subcatalog in enumerate(event["subcatalogs"]):
        if not isinstance(subcatalog, dict):
            raise ValueError(
                f"Subcatalog at index {index} must be an object."
            )

        if not subcatalog.get("path"):
            raise ValueError(
                f"Subcatalog at index {index} must contain a non-empty 'path'."
            )

        if "records" not in subcatalog:
            raise ValueError(
                f"Subcatalog '{subcatalog['path']}' must contain 'records'."
            )

        if not isinstance(subcatalog["records"], list):
            raise ValueError(
                f"'records' in subcatalog '{subcatalog['path']}' must be a list."
            )

        if not subcatalog["records"]:
            raise ValueError(
                f"'records' in subcatalog '{subcatalog['path']}' cannot be empty."
            )

        if not all(
            isinstance(record, dict)
            for record in subcatalog["records"]
        ):
            raise ValueError(
                f"Every item in 'records' for '{subcatalog['path']}' "
                "must be an object."
            )

        rows_per_file = subcatalog.get(
            "rows_per_file",
            DEFAULT_ROWS_PER_FILE,
        )

        if (
            not isinstance(rows_per_file, int)
            or isinstance(rows_per_file, bool)
            or rows_per_file <= 0
        ):
            raise ValueError(
                f"'rows_per_file' for '{subcatalog['path']}' "
                "must be a positive integer."
            )


def lambda_handler(event, context):
    logger.info(
        "Received event with %s subcatalog(s).",
        len(event.get("subcatalogs", [])),
    )

    validate_event(event)

    bucket = event.get("bucket") or os.environ.get("S3_BUCKET")

    if not bucket:
        raise ValueError(
            "S3 bucket is required. Set event['bucket'] "
            "or the S3_BUCKET environment variable."
        )

    generated_files = []

    for subcatalog_config in event["subcatalogs"]:
        files = process_subcatalog(
            bucket=bucket,
            config=subcatalog_config,
        )
        generated_files.extend(files)

    response = {
        "bucket": bucket,
        "generated_files": generated_files,
        "generated_files_count": len(generated_files),
    }

    logger.info(
        "Parquet generation finished: %s",
        json.dumps(response),
    )

    return response