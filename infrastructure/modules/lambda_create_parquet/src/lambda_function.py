import json
import logging
import os
import random
import uuid

from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

DEFAULT_ROWS_PER_FILE = 100
DEFAULT_FILES = 1

def generate_mock_data(
    rows_count: int,
    subcatalog: str,
) -> pa.Table:

    now = datetime.now(timezone.utc)

    data = {
        "id": [],
        "name": [],
        "amount": [],
        "active": [],
        "source_catalog": [],
        "created_at": [],
    }

    for _ in range(rows_count):
        record_id = random.randint(1, 10_000_000)

        data["id"].append(record_id)
        data["name"].append(f"mock-{record_id}")
        data["amount"].append(round(random.uniform(1, 10000), 2))
        data["active"].append(random.choice([True, False]))
        data["source_catalog"].append(subcatalog)
        data["created_at"].append(now)

    return pa.table(data)


def write_parquet_to_s3(
    table: pa.Table,
    bucket: str,
    prefix: str,
) -> str:

    file_id = uuid.uuid4().hex

    filename = f"mock-{file_id}.parquet"
    local_path = Path("/tmp") / filename

    pq.write_table(
        table,
        local_path,
        compression="snappy",
    )

    s3_key = f"{prefix.rstrip('/')}/{filename}"

    logger.info(
        "Uploading parquet file: s3://%s/%s",
        bucket,
        s3_key,
    )

    s3.upload_file(
        str(local_path),
        bucket,
        s3_key,
    )

    local_path.unlink(missing_ok=True)

    return f"s3://{bucket}/{s3_key}"


def process_subcatalog(
    bucket: str,
    config: dict,
) -> list[str]:

    path = config["path"]

    rows_per_file = config.get(
        "rows_per_file",
        DEFAULT_ROWS_PER_FILE,
    )

    number_of_files = config.get(
        "files",
        DEFAULT_FILES,
    )

    generated_files = []

    for _ in range(number_of_files):

        table = generate_mock_data(
            rows_count=rows_per_file,
            subcatalog=path,
        )

        s3_uri = write_parquet_to_s3(
            table=table,
            bucket=bucket,
            prefix=path,
        )

        generated_files.append(s3_uri)

    return generated_files


def validate_event(event: dict) -> None:

    if "subcatalogs" not in event:
        raise ValueError("Event must contain 'subcatalogs'.")

    if not isinstance(event["subcatalogs"], list):
        raise ValueError("'subcatalogs' must be a list.")

    if not event["subcatalogs"]:
        raise ValueError("'subcatalogs' cannot be empty.")

    for subcatalog in event["subcatalogs"]:

        if not isinstance(subcatalog, dict):
            raise ValueError(
                "Each subcatalog must be an object."
            )

        if "path" not in subcatalog:
            raise ValueError(
                "Each subcatalog must contain 'path'."
            )


def lambda_handler(event, context):
    logger.info(
        "Received event: %s",
        json.dumps(event),
    )

    validate_event(event)

    bucket = os.environ.get("S3_BUCKET")

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
        "Generation finished: %s",
        json.dumps(response),
    )

    return response