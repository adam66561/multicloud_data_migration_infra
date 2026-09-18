
# s3_io.py
from __future__ import annotations

from io import BytesIO
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


def read_parquet(bucket: str, key: str, pk_cols: list[str]) -> pd.DataFrame | None:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key,)
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
        dtype_backend="pyarrow"
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

    if df.empty:
        logger.warning(
            "Parquet file is empty: s3://%s/%s",
            bucket,
            key,
        )
        return None
    
    return df