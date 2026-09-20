# test_read_parquet.py
from io import BytesIO
from unittest.mock import patch

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
import sys
sys.path.insert(0, "../../src")
from s3_io import read_parquet

# | S3 object is available and CDC data is valid 
# also mixed case columns are normalized to lower case
@patch("s3_io.pq.read_table")
@patch("s3_io.s3_client")
def test_read_parquet_valid_table(s3_client_mock, read_parquet_mock):
    s3_client_mock.get_object.return_value = {
        "Body": BytesIO(b"not-real-parquet-for-this-unit-test")
    }
    read_parquet_mock.return_value = pa.table(
        {
            "ORDER_ID": [101, 102, 103],
            "CUSTOMER_ID": [10, 20, 30],
            "OP": ["i", "U", "d"],
            "OPTIME": [
                "2026-09-18T10:00:00Z",
                "2026-09-18T10:01:00Z",
                "2026-09-18T10:02:00Z",
            ],
        }
    )

    result, num_rows = read_parquet(
        bucket="raw-cdc-bucket",
        key="cdc/sales/orders/part-00001.parquet",
        pk_cols=["order_id", "customer_id"],
    )

    s3_client_mock.get_object.assert_called_once_with(
        Bucket="raw-cdc-bucket",
        Key="cdc/sales/orders/part-00001.parquet",
    )

    read_parquet_mock.assert_called_once()
    assert result.column_names == [
        "order_id",
        "customer_id",
        "op",
        "optime",
    ]
    assert result["op"].to_pylist() == ["I", "U", "D"]
    assert result["order_id"].to_pylist() == [101, 102, 103]
    assert num_rows == 3


# | op, or optime column is missing    | Raises RuntimeError naming missing columns  |
@patch("s3_io.pq.read_table")
@patch("s3_io.s3_client")
def test_read_parquet_missing_op_column(s3_client_mock, read_parquet_mock):
    s3_client_mock.get_object.return_value = {
        "Body": BytesIO(b"not-real-parquet-for-this-unit-test")
    }
    read_parquet_mock.return_value = pa.table(
        {
            "ORDER_ID": [101, 102, 103],
            "CUSTOMER_ID": [10, 20, 30],
            "OPTIME": [
                "2026-09-18T10:00:00Z",
                "2026-09-18T10:01:00Z",
                "2026-09-18T10:02:00Z",
            ],
        }
    )

    with pytest.raises(RuntimeError, match="Missing required columns in Parquet: .*"):
        read_parquet(
            bucket="raw-cdc-bucket",
            key="cdc/sales/orders/part-00001.parquet",
            pk_cols=["order_id", "customer_id"],
        )

# primary key column is missing    | Raises RuntimeError naming missing columns  |
@patch("s3_io.pq.read_table")
@patch("s3_io.s3_client")
def test_read_parquet_missing_pk_column(s3_client_mock, read_parquet_mock):
    s3_client_mock.get_object.return_value = {
        "Body": BytesIO(b"not-real-parquet-for-this-unit-test")
    }
    read_parquet_mock.return_value = pa.table(
        {
            "CUSTOMER_ID": [10, 20, 30],
            "OP": ["I", "U", "D"],
            "OPTIME": [
                "2026-09-18T10:00:00Z",
                "2026-09-18T10:01:00Z",
                "2026-09-18T10:02:00Z",
            ],
        }
    )

    with pytest.raises(RuntimeError, match="Missing required columns in Parquet: .*"):
        read_parquet(
            bucket="raw-cdc-bucket",
            key="cdc/sales/orders/part-00001.parquet",
            pk_cols=["order_id", "customer_id"],
        )


# | One or more primary-key fields are null           | Raises RuntimeError                         |
@patch("s3_io.pq.read_table")
@patch("s3_io.s3_client")
def test_read_parquet_null_pk_column(s3_client_mock, read_parquet_mock):
    s3_client_mock.get_object.return_value = {
        "Body": BytesIO(b"not-real-parquet-for-this-unit-test")
    }
    read_parquet_mock.return_value = pa.table(
        {
            "ORDER_ID": [101, None, 103],
            "CUSTOMER_ID": [10, 20, 30],
            "OP": ["I", "U", "D"],
            "OPTIME": [
                "2026-09-18T10:00:00Z",
                "2026-09-18T10:01:00Z",
                "2026-09-18T10:02:00Z",
            ],
        }
    )

    with pytest.raises(RuntimeError, match="Parquet contains rows with null primary key: .*"):
        read_parquet(
            bucket="raw-cdc-bucket",
            key="cdc/sales/orders/part-00001.parquet",
            pk_cols=["order_id", "customer_id"],
        )
        

# | Any unsupported operation—for example X or DELETE | Raises RuntimeError with the invalid values |
@patch("s3_io.pq.read_table")
@patch("s3_io.s3_client")
def test_read_parquet_unsupported_op(s3_client_mock, read_parquet_mock):
    s3_client_mock.get_object.return_value = {
        "Body": BytesIO(b"not-real-parquet-for-this-unit-test")
    }
    read_parquet_mock.return_value = pa.table(
        {
            "ORDER_ID": [101, 102, 103],
            "CUSTOMER_ID": [10, 20, 30],
            "OP": ["I", "X", "DELETE"],
            "OPTIME": [
                "2026-09-18T10:00:00Z",
                "2026-09-18T10:01:00Z",
                "2026-09-18T10:02:00Z",
            ],
        }
    )

    with pytest.raises(RuntimeError, match="Unsupported operations found: .*"):
        read_parquet(
            bucket="raw-cdc-bucket",
            key="cdc/sales/orders/part-00001.parquet",
            pk_cols=["order_id", "customer_id"],
        )


# valid table collumns but empty
@patch("s3_io.pq.read_table")
@patch("s3_io.s3_client")
def test_read_parquet_returns_empty_table_when_schema_is_valid(
    s3_client_mock,
    read_parquet_mock,
):
    s3_client_mock.get_object.return_value = {
        "Body": BytesIO(b"not-real-parquet-for-this-unit-test")
    }

    read_parquet_mock.return_value = pa.table(
        {
            "ORDER_ID": pa.array([], type=pa.int64()),
            "CUSTOMER_ID": pa.array([], type=pa.int64()),
            "OP": pa.array([], type=pa.string()),
            "OPTIME": pa.array([], type=pa.string()),
        }
    )

    result, num_rows = read_parquet(
        bucket="raw-cdc-bucket",
        key="cdc/sales/orders/empty-part.parquet",
        pk_cols=["order_id", "customer_id"],
    )

    assert result is None
    assert num_rows == 0


# errors
def make_client_error(error_code: str, operation_name: str = "GetObject") -> ClientError:
    return ClientError(
        error_response={
            "Error": {
                "Code": error_code,
                "Message": f"S3 error: {error_code}",
            }
        },
        operation_name=operation_name,
    )

@pytest.mark.parametrize("error_code", ["NoSuchKey", "404"])
@patch("s3_io.s3_client")
def test_read_parquet_returns_none_for_missing_s3_object(
    s3_client_mock,
    error_code,
):
    s3_client_mock.get_object.side_effect = make_client_error(error_code)

    result, num_rows = read_parquet(
        bucket="raw-cdc-bucket",
        key="cdc/sales/orders/missing-file.parquet",
        pk_cols=["order_id", "customer_id"],
    )

    assert result is None
    assert num_rows == 0

@pytest.mark.parametrize(
    "error_code",
    [
        "AccessDenied",
        "SlowDown",
        "InternalError",
    ],
)
@patch("s3_io.s3_client")
def test_read_parquet_reraises_non_missing_s3_errors(
    s3_client_mock,
    error_code,
):
    s3_client_mock.get_object.side_effect = make_client_error(error_code)

    with pytest.raises(ClientError) as exc_info:
        read_parquet(
            bucket="raw-cdc-bucket",
            key="cdc/sales/orders/part-00001.parquet",
            pk_cols=["order_id", "customer_id"],
        )

    assert exc_info.value.response["Error"]["Code"] == error_code