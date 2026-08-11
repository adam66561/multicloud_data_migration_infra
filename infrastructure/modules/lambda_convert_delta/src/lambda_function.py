import logging
import os
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError
from deltalake import DeltaTable, convert_to_deltalake

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')
s3_client = boto3.client("s3")

def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/").rstrip("/")

def build_target_s3_uri(source_uri: str) -> str:
    source_bucket, source_key = parse_s3_uri(source_uri)
    return f"s3://{source_bucket}/delta/{source_key}".rstrip("/")

def convert_copied_dataset_to_delta(target_uri: str) -> int:
    try:
        logger.info("Processing table location: %s", target_uri)
        convert_to_deltalake(uri=target_uri, mode='ignore')
        dt = DeltaTable(target_uri)
        version = dt.version()
        logger.info("Delta table version for %s: %s", target_uri, version)
        return version
    except Exception:
        logger.exception("Failed converting table location: %s", target_uri)
        raise

def get_tables_locations(catalog_id: str, database: str) -> list[str]:
    results = []
    try:
        paginator = glue_client.get_paginator("get_tables")
        
        pages = paginator.paginate(
            CatalogId=catalog_id,
            DatabaseName=database,
        )

        for page in pages:
            for table in page.get("TableList", []):
                result = process_table(table)
                results.append(result)

        return results

    except glue_client.exceptions.EntityNotFoundException:
        logger.error("Glue database not found: %s", database)
        raise
    except ClientError:
        logger.exception("Glue client error for database: %s", database)
        raise

def process_table(table: dict) -> dict:
    table_name = table.get("Name")
    storage_descriptor = table.get("StorageDescriptor", {})
    source_uri = storage_descriptor.get("Location")

    if not source_uri:
        logger.warning("Skipping table %s because StorageDescriptor.Location is missing", table_name)
        return {
            "table_name": table_name,
            "status": "skipped",
            "reason": "missing_location",
        }

    if not source_uri.startswith("s3://"):
        logger.warning("Skipping table %s because location is not an S3 URI: %s", table_name, source_uri)
        return {
            "table_name": table_name,
            "status": "skipped",
            "reason": "invalid_location",
            "source_uri": source_uri,
        }

    delta_version = convert_copied_dataset_to_delta(source_uri)

    return {
        "table_name": table_name,
        "status": "converted",
        "source_uri": source_uri,
        "delta_version": delta_version,
    }


def lambda_handler(event, context):
    try:

        catalog_id = os.environ.get('GLUE_CATALOG_ID')
        glue_databases = [
            db.strip()
            for db in os.environ["GLUE_DATABASE_NAME"].split(",")
            if db.strip()
        ]

        results = []

        for glue_database in glue_databases:
            logger.info("Processing Glue database: %s", glue_database)

            try:
                table_results = get_tables_locations(catalog_id, glue_database)
                results.append({
                    "database": glue_database,
                    "status": "succeeded",
                    "reason": "database_migrated_and_converted",
                    "tables": table_results,
                })
            except glue_client.exceptions.EntityNotFoundException:
                logger.error("Glue database not found: %s", glue_database)
                results.append({
                    "database": glue_database,
                    "status": "failed",
                    "reason": "database_not_found",
                })
                continue
            except ClientError as ex:
                logger.exception("Glue client error for database %s: %s", glue_database, str(ex))
                results.append({
                    "database": glue_database,
                    "status": "failed",
                    "reason": "glue_client_error",
                })
                continue

        logger.info("Conversion results: %s", results)
        return results
    except KeyError as ex:
        logger.error(ex)
        raise ex