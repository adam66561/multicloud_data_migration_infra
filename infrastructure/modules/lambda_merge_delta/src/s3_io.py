
# s3_io.py
from __future__ import annotations

from io import BytesIO
import logging

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


def read_parquet(bucket: str, key: str, pk_cols: list[str]) -> tuple[pa.Table | None, int]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key,)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            logger.warning("CDC file no longer exists; skipping: s3://%s/%s", bucket, key)
            return None, 0

        logger.exception("Unable to read CDC object: s3://%s/%s; code=%s", bucket, key, error_code)
        raise

    table = pq.read_table(
        BytesIO(response["Body"].read())
    )

    table = table.rename_columns([str(column).lower() for column in table.column_names])

    if table.num_rows == 0:
        logger.warning("Parquet file is empty: s3://%s/%s", bucket, key)
        return None, 0

    required_columns = set(pk_cols + ["op", "optime"])
    missing_columns = required_columns - set(table.column_names)
    if missing_columns:
        raise RuntimeError(
            f"Missing required columns in Parquet: "
            f"{sorted(missing_columns)}"
        )

    for pk_col in pk_cols:
        if table[pk_col].null_count > 0:
            raise RuntimeError(f"Parquet contains rows with null primary key: {pk_col}")

    op_column = pc.utf8_upper(table["op"])
    op_index = table.schema.get_field_index("op")
    table = table.set_column(op_index, "op", op_column,)

    valid_ops = pa.array(["I", "U", "D"])
    valid_mask = pc.is_in(table["op"], value_set=valid_ops)
    invalid_mask = pc.invert(valid_mask)
    if pc.any(invalid_mask).as_py():
        invalid_ops = (table.filter(invalid_mask).select(["op"]).column("op").unique().to_pylist())
        raise RuntimeError(f"Unsupported operations found: {invalid_ops}")
    
    return table, table.num_rows