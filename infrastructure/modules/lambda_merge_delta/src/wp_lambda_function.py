import boto3
import base64
import json
import pandas as pd
import pyarrow as pa
import logging
import os
import time
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Literal
import random
from io import BytesIO
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOTO3_SESSION = boto3.Session()
PRIMARY_KEYS_FILE_NAME = 'primary_keys.json'

def load_primary_keys(s3_conf_bucket: str, s3_conf_key: str):
    try:
        s3_client = boto3.client('s3')
        s3_object = s3_client.get_object(Bucket=s3_conf_bucket, Key=s3_conf_key)
        s3_content = s3_object['Body'].read().decode('utf-8')
        return json.loads(s3_content)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        return {}

def reduce_events_for_key(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    state = None
    for event in events:
        if event["op"] == "UPSERT":
            state = event["data"].copy()
        elif event["op"] == "DELETE":
            state = None
        else:
            raise ValueError(f"Unsupported op: {event['op']}")

    return state

def build_final_states(kinesis_event: Dict[str, Any], primary_keys: Dict[str, List[str]],) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[str], Optional[str]]:
    grouped = defaultdict(list)
    schema_name = None
    table_name = None

    for record in kinesis_event["Records"]:
        try:
            record_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            record_json = json.loads(record_data)

            data_obj = record_json.get("data", {}) or {}
            if isinstance(data_obj, dict) and "_doc" in data_obj and isinstance(data_obj["_doc"], dict):
                business_source = data_obj["_doc"]
            else:
                business_source = data_obj

            business_data = {k.lower(): v for k, v in business_source.items()}
            metadata = record_json.get('metadata', {})
            operation = metadata.get('operation')
            
            schema_name_candidate = metadata.get('schema-name').lower()
            table_name_candidate = metadata.get('table-name').lower()

            table_key = f"{schema_name_candidate}.{table_name_candidate}".lower()
            pk_cols = [c.lower() for c in primary_keys.get(table_key, [])]
            if not pk_cols:
                raise RuntimeError(f"No primary key defined for {table_key}")

            missing_pk = [c for c in pk_cols if c not in business_data or business_data[c] is None]
            if missing_pk:
                raise RuntimeError(
                    f"CDC event missing PK columns for {table_key}: {missing_pk}"
                )
            
            pk = tuple(business_data[c] for c in pk_cols)

            if operation in ("insert", "update"):
                normalized_op = "UPSERT"
            elif operation == "delete":
                normalized_op = "DELETE"
            else:
                raise RuntimeError(f"Unsupported operation '{operation}' for {table_key}")

            event = {
                "pk": pk,
                "op": normalized_op,
                "data": business_data,
            }

            grouped[pk].append(event)
            schema_name = schema_name_candidate
            table_name = table_name_candidate

        except Exception as e:
            logger.error(f"Error decoding record: {e}")

    final_upserts = []
    final_deletes = []

    for pk, events in grouped.items():
        final_state = reduce_events_for_key(events)

        if final_state is None:
            delete_row = {}
            table_key = f"{schema_name}.{table_name}".lower()
            pk_cols = [c.lower() for c in primary_keys.get(table_key, [])]
            for i, col in enumerate(pk_cols):
                delete_row[col] = pk[i]
            final_deletes.append(delete_row)
        else:
            final_upserts.append(final_state)

    return final_upserts, final_deletes, schema_name, table_name



def _run_merge_once(df: pd.DataFrame, full_s3_path: str, pk_cols: List[str], operation: Literal["upsert", "delete"]):
    dt = DeltaTable(full_s3_path)
    merge_predicate = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])

    if operation == "upsert":
        source_table = pa.Table.from_pandas(df, preserve_index=False)
        col_map = {c: f"source.{c}" for c in df.columns}
        (
            dt.merge(
                source=source_table,
                predicate=merge_predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(updates=col_map)
            .when_not_matched_insert(updates=col_map)
            .execute()
        )
    elif operation == "delete":
        source_table = pa.Table.from_pandas(df[pk_cols], preserve_index=False)
        (
            dt.merge(
                source=source_table,
                predicate=merge_predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_delete()
            .execute()
        )

    else:
        raise ValueError(f"Unsupported operation: {operation}")

    return dt, merge_predicate

def _execute_delta_merge_with_retry(
    *,
    df: pd.DataFrame,
    full_s3_path: str,
    schema_name: str,
    table_name: str,
    primary_keys: Dict[str, List[str]],
    operation: Literal["upsert", "delete"],
    max_attempts: int = 3,
):
    table_key = f"{schema_name}.{table_name}".lower()
    pk_cols = [c.lower() for c in primary_keys.get(table_key, [])]
    if not pk_cols:
        raise RuntimeError(f"No primary key defined for {table_key}")

    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    missing_pk = [c for c in pk_cols if c not in df.columns]
    if missing_pk:
        raise RuntimeError(f"Dataframe missing PK columns for {table_key}: {missing_pk}")

    last_exception: Optional[Exception] = None
    last_dt: Optional[DeltaTable] = None
    merge_predicate: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        try:
            last_dt, merge_predicate = _run_merge_once(
                full_s3_path=full_s3_path,
                df=df,
                pk_cols=pk_cols,
                operation=operation,
            )

            if operation == "upsert":
                logger.info(f"SUCCESS: Upserts written to {full_s3_path}")
            else:
                logger.info(f"SUCCESS: Deletes applied to {full_s3_path}")
            return

        except Exception as e:
            last_exception = e

            if attempt == max_attempts:
                break

            sleep_seconds = min(8.0, (2 ** (attempt - 1)) + random.uniform(0.1, 0.8))
            logger.warning(
                f"Retryable {operation} failure on {full_s3_path}, "
                f"attempt {attempt}/{max_attempts}: {e}. "
                f"Sleeping {sleep_seconds:.2f}s before retry."
            )
            time.sleep(sleep_seconds)

    logger.error(f"CRITICAL: Failed to {operation} in S3 Delta Table. Error: {last_exception}")
    if last_dt is not None:
        logger.info(f"Delta schema: {[f.name for f in last_dt.schema().fields]}")
    logger.info(f"PK cols: {pk_cols}")
    logger.info(f"Merge predicate: {merge_predicate}")
    if last_exception is None:
        raise RuntimeError(f"Unknown failure during {operation} for {full_s3_path}")
    raise last_exception


def _process_s3_record(
    s3_record: Dict[str, Any],
    *,
    s3_bucket_base_path: str,
    primary_keys: Dict[str, List[str]],
):
    """
    Expected record shape:
    {
      "eventSource": "aws:s3",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {
          "name": "source-bucket"
        },
        "object": {
          "key": "path/file.parquet"
        }
      }
    }
    """

    try:
        bucket = s3_record["s3"]["bucket"]["name"]
        key = unquote_plus(s3_record["s3"]["object"]["key"])
    except KeyError as e:
        raise RuntimeError(f"Invalid S3 event record, missing field: {e}") from e

    if not key.lower().endswith(".parquet"):
        logger.info(f"Ignoring non-Parquet objects3://{bucket}/{key}")
        return

    schema_name, table_name = (_extract_schema_table_from_s3_key(key))

    logger.info(f"Resolved source as {schema_name}.{table_name} from S3 key")

    parquet_df = _read_parquet_from_s3(bucket=bucket, key=key)

    upsert_buffer, deletes_buffer = (
        build_final_states_from_parquet(
            parquet_df,
            schema_name=schema_name,
            table_name=table_name,
            primary_keys=primary_keys,
        )
    )

    if not deletes_buffer and not upsert_buffer:
        logger.info(f"No valid CDC rows found in s3://{bucket}/{key}")
        return

    df_deletes = (
        pd.DataFrame(deletes_buffer)
        if deletes_buffer
        else pd.DataFrame()
    )

    df_upsert = (
        pd.DataFrame(upsert_buffer)
        if upsert_buffer
        else pd.DataFrame()
    )

    base_path = s3_bucket_base_path.rstrip("/")

    full_s3_path = (f"{base_path}/{schema_name}/{table_name}/")

    if not df_upsert.empty:
        _execute_delta_merge_with_retry(
            df=df_upsert,
            full_s3_path=full_s3_path,
            schema_name=schema_name,
            table_name=table_name,
            primary_keys=primary_keys,
            operation="upsert",
            max_attempts=3,
        )

    if not df_deletes.empty:
        _execute_delta_merge_with_retry(
            df=df_deletes,
            full_s3_path=full_s3_path,
            schema_name=schema_name,
            table_name=table_name,
            primary_keys=primary_keys,
            operation="delete",
            max_attempts=3,
        )

    total = (len(upsert_buffer)+ len(deletes_buffer))

    logger.info(f"Successfully processed {total} CDC rows from s3://{bucket}/{key}for {schema_name}.{table_name}")


def lambda_handler(event, context):
    try:
        s3_config_bucket = os.environ['S3_CONFIG_BUCKET']
        s3_bucket_base_path = os.environ['S3_BASE_PATH']

    except KeyError as e:
        logger.error(f"Missing env variable: {e}")
        raise RuntimeError(f"Configuration error: {e}")

    primary_keys = load_primary_keys(s3_config_bucket, PRIMARY_KEYS_FILE_NAME)


    records = event.get("Records", [])

    if not records:
        logger.info("No S3 records found in Lambda event")
        return {"statusCode": 200,"processed": 0}

    processed = 0
    for record in records:
        event_source = record.get("eventSource")

        if event_source != "aws:s3":
            logger.warning(
                f"Ignoring unsupported event source: "
                f"{event_source}"
            )
            continue

        _process_s3_record(
            record,
            s3_bucket_base_path=s3_bucket_base_path,
            primary_keys=primary_keys,
        )

        processed += 1


    return {'statusCode': 200, 'processed': processed}