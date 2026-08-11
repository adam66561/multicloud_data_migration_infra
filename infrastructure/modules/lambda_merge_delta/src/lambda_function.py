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

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOTO3_SESSION = boto3.Session()
CONFIG_FILE_NAME = 'schema_config.json'
PRIMARY_KEYS_FILE_NAME = 'primary_keys.json'

RETRYABLE_ERROR_SNIPPETS = (
    "versionalreadyexists",
    "version already exists",
    "concurrent",
    "temporarily unavailable",
    "timeout",
    "timed out",
    "throttl",
    "toomanyrequests",
)

def align_df_to_schema_config(df: pd.DataFrame, schema_name: str,table_name: str, mapping_config: dict) -> pd.DataFrame:
    table_key = f"{schema_name}.{table_name}".lower()
    table_config = mapping_config.get(table_key, {})

    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    if not table_config:
        logger.info(f"No schema config found for {table_key}, leaving columns as-is.")
        return df

    expected_cols = [c.lower() for c in table_config.keys()]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    extra_cols = [c for c in df.columns if c not in expected_cols]

    return df[expected_cols + extra_cols]

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

def build_final_states(kinesis_event: Dict[str, Any], expected_schema_name: str, expected_table_name: str, primary_keys: Dict[str, List[str]],) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[str], Optional[str]]:
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

            if schema_name_candidate != expected_schema_name or table_name_candidate != expected_table_name:
                continue

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


def load_schema_config(s3_conf_bucket: str, s3_conf_key: str):
    try:
        s3_client = boto3.client('s3')
        s3_object = s3_client.get_object(Bucket=s3_conf_bucket, Key=s3_conf_key)
        s3_content = s3_object['Body'].read().decode('utf-8')
        return json.loads(s3_content)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        return {}

def apply_schema_casting(df: pd.DataFrame, schema_name: str, table_name: str, mapping_config: dict) -> pd.DataFrame:
    table_key = f"{schema_name}.{table_name}".lower()
    table_config = mapping_config.get(table_key, {})
    if not table_config:
        logger.info(f"No custom mapping found for {table_key}.")
        return df.astype(str)

    for col_name, target_type in table_config.items():
        col_name = col_name.lower()
        if col_name in df.columns:
            try:
                if target_type == 'int' or target_type == "bigint":
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                
                elif target_type == 'date':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date

                elif target_type == "decimal":
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

                elif target_type == "timestamp":
                    # s = pd.to_datetime(df[col_name], errors="coerce", utc=True)
                    # df[col_name] = s.dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[us]")
                    s_raw = df[col_name].astype("string")
                    s_ts = pd.to_datetime(s_raw, errors="coerce", utc=True)
                    df[col_name] = s_ts
                    df[f"{col_name}_raw"] = s_raw

                elif target_type == 'string':
                    df[col_name] = df[col_name].replace({'nan': None, '<NA>': None, 'None': None, 'null': None})
                    df[col_name] = df[col_name].astype('string')

            except Exception as e:
                logger.warning(f"Error casting column '{col_name}' to '{target_type}': {e}")
    return df


def _is_retryable_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(token in msg for token in RETRYABLE_ERROR_SNIPPETS)

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

    if operation == "upsert":
        try:
            DeltaTable(full_s3_path)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            write_deltalake(full_s3_path, df, mode="append")
            logger.info(f"Bootstrap Delta table created at {full_s3_path}")
            return

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

            if attempt == max_attempts or not _is_retryable_error(e):
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


def lambda_handler(event, context):
    try:
        s3_config_bucket = os.environ['S3_CONFIG_BUCKET']
        s3_bucket_base_path = os.environ['S3_BASE_PATH']
        expected_schema_name = os.environ.get('SOURCE_SCHEMA_NAME')
        expected_table_name = os.environ.get('SOURCE_TABLE_NAME')

    except KeyError as e:
        logger.error(f"Missing env variable: {e}")
        raise RuntimeError(f"Configuration error: {e}")

    schema_mapping = load_schema_config(s3_config_bucket, CONFIG_FILE_NAME)
    primary_keys = load_schema_config(s3_config_bucket, PRIMARY_KEYS_FILE_NAME)

    upsert_buffer, deletes_buffer, schema_name, table_name = build_final_states(
        event,
        expected_schema_name=expected_schema_name,
        expected_table_name=expected_table_name,
        primary_keys=primary_keys
    )

    if not deletes_buffer and not upsert_buffer:
        logger.info("No valid records found for processing.")
        return {'statusCode': 200}

    df_deletes = pd.DataFrame(deletes_buffer) if deletes_buffer else pd.DataFrame()
    df_upsert = pd.DataFrame(upsert_buffer) if upsert_buffer else pd.DataFrame()

    if schema_name and table_name:
        if not df_upsert.empty:
            df_upsert = align_df_to_schema_config(df_upsert, schema_name, table_name, schema_mapping)
            df_upsert = apply_schema_casting(df_upsert, schema_name, table_name, schema_mapping)
        if not df_deletes.empty:
            df_deletes = align_df_to_schema_config(df_deletes, schema_name, table_name, schema_mapping)
            df_deletes = apply_schema_casting(df_deletes, schema_name, table_name, schema_mapping)
    else:
        if not df_upsert.empty:
            df_upsert = align_df_to_schema_config(df_upsert, schema_name, table_name, schema_mapping)
            df_upsert = df_upsert.astype(str)
        if not df_deletes.empty:
            df_deletes = align_df_to_schema_config(df_deletes, schema_name, table_name, schema_mapping)
            df_deletes = df_deletes.astype(str)

    if not s3_bucket_base_path.endswith('/'):
        s3_bucket_base_path += '/'

    full_s3_path = f"{s3_bucket_base_path}{table_name}/"
    
    try:
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

        total = len(upsert_buffer) + len(deletes_buffer)
        logger.info(f"Successfully processed {total} records for {table_name}.")
    except Exception as e:
        raise

    return {'statusCode': 200}
