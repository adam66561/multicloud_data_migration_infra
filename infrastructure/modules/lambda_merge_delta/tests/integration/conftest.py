# conftest.py
import json
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pytest

from deltalake import write_deltalake
from moto import mock_aws

@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

@pytest.fixture
def lambda_environment(monkeypatch):
    monkeypatch.setenv("S3_SOURCE_BUCKET", "source-bucket")
    monkeypatch.setenv("S3_TARGET_BUCKET", "target-bucket")
    monkeypatch.setenv("S3_CONFIG_BUCKET", "config-bucket")
    monkeypatch.setenv("S3_CONFIG_KEY", "primary_keys.json")

    monkeypatch.setenv("AUDIT_LOGS", "true")
    monkeypatch.setenv("AUDIT_TABLE_NAME", "merge-audit")

    monkeypatch.setenv(
        "PREFIX_MAPPING",
        json.dumps({
            "schema1": "prefix1/schema_target1",
            "schema2": "prefix2/schema_target2",
        }),
    )

    monkeypatch.setenv(
        "DATE_PARTITION_SUBFOLDER_COUNT",
        "3",
    )

@pytest.fixture
def mocked_aws(
    aws_credentials,
    lambda_environment,
):
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-central-1",)
        dynamodb = boto3.resource("dynamodb", region_name="eu-central-1",)

        for bucket in ["source-bucket", "target-bucket", "config-bucket"]:
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-central-1"})

        primary_keys = {"schema_target1.table1": ["id1","id2"], "schema_target2.table2": ["id3"]}
        s3.put_object(Bucket="config-bucket", Key="primary_keys.json", Body=json.dumps(primary_keys))

        table = dynamodb.create_table(
            TableName="merge-audit",
            KeySchema=[
                {
                    "AttributeName": "file_id",
                    "KeyType": "HASH",
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "file_id",
                    "AttributeType": "S",
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        yield {
            "s3": s3,
            "dynamodb": dynamodb,
            "audit_table": table,
        }

@pytest.fixture
def create_delta_table(tmp_path):
    def _create(rows):
        delta_path = tmp_path / "delta"

        df = pd.DataFrame(rows)

        arrow_table = pa.Table.from_pandas(
            df,
            preserve_index=False,
        )

        write_deltalake(str(delta_path), arrow_table, mode="overwrite")
        return delta_path
    return _create

@pytest.fixture
def upload_parquet(mocked_aws):
    def _upload(rows):
        df = pd.DataFrame(rows)

        buffer = BytesIO()

        df.to_parquet(buffer, engine="pyarrow", index=False)

        key = ("schema1/table1/2026/09/18/0001.parquet")

        mocked_aws["s3"].put_object(Bucket="source-bucket", Key=key, Body=buffer.getvalue())

        return key

    return _upload

@pytest.fixture
def create_lambda_event():
    def _create(key):
        eventbridge_event = {
            "version": "0",
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": {
                    "name": "source-bucket",
                },
                "object": {
                    "key": key,
                },
            },
        }

        return {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(eventbridge_event),
                }
            ]
        }

    return _create

@pytest.fixture
def run_lambda(monkeypatch, mocked_aws):
    def _run(event, delta_path):
        import sys
        sys.path.insert(0, "../../src")
        import lambda_function

        monkeypatch.setattr(
            lambda_function,
            "build_target_path",
            lambda settings, target_prefix, table_name: str(delta_path),
        )

        return lambda_function.lambda_handler(event, None)

    return _run