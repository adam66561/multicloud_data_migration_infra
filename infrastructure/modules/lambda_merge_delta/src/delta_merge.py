# delta_merge.py
from __future__ import annotations

import logging
import random
import time
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

logger = logging.getLogger(__name__)

TRANSIENT_MERGE_ERROR_MARKERS = (
    "concurrent",
    "conflict",
    "transaction",
    "temporarily unavailable",
    "timeout",
    "connection reset",
    "slow down",
)

def is_transient_merge_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in TRANSIENT_MERGE_ERROR_MARKERS)

def delta_table_exists(path: str) -> bool:
    return DeltaTable.is_deltatable(path)

def merge_once(
        table: pa.Table,
        target_path: str,
        pk_cols: list[str]
    ) -> dict[str, Any]:

    dt = DeltaTable(target_path)

    merge_predicate = " AND ".join(
    [
        f"target.`{column}` = source.`{column}`"
        for column in pk_cols
    ]
    )

    target_columns = {
        field.name
        for field in dt.schema().fields
    }

    new_columns = [
        column
        for column in table.column_names
        if column not in target_columns
    ]

    logger.info(
        f"Starting merge for {target_path}; "
        f"rows={table.num_rows}; "
        f"pk_cols={pk_cols}; "
        f"new_columns={new_columns}; "
        f"predicate={merge_predicate}"
    )

    update_map = {
        column: (
            f"COALESCE(source.`{column}`, target.`{column}`)"
            if column in target_columns
            else f"source.`{column}`"
        )
        for column in table.column_names
        if column not in pk_cols
    }

    replace_map = {
        column: f"source.`{column}`"
        for column in table.column_names
        if column not in pk_cols
    }

    insert_map = {
        column: f"source.`{column}`"
        for column in table.column_names
    }

    metrics = (
        dt.merge(
            source=table,
            predicate=merge_predicate,
            source_alias="source",
            target_alias="target",
            merge_schema=True,
        )
        .when_matched_delete(predicate=(
            "source.op = 'D' "
            "AND source.optime >= target.optime")
        )
        .when_matched_update(predicate=(
            "source.op = 'I' "
            "AND source.optime >= target.optime"),
            updates=replace_map
        )        
        .when_matched_update(predicate=(
            "source.op = 'U' "
            "AND source.optime >= target.optime"),
            updates=update_map
        )
        .when_not_matched_insert(predicate=(
            "source.op IN ('I', 'U')"), 
            updates=insert_map
        )
        .execute()
    )

    return metrics


def merge_with_retry(
    table: pa.Table,
    target_path: str,
    pk_cols: list[str],
    max_attempts: int = 3,
) -> tuple[dict[str, Any], int]:
    for attempt in range(1, max_attempts + 1):
        try:
            metrics = merge_once(
                table=table,
                target_path=target_path,
                pk_cols=pk_cols,
            )

            logger.info(f"SUCCESS: merge completed for {target_path}")
            return metrics, attempt

        except Exception as e:
            if not is_transient_merge_error(e):
                logger.error(f"FAILURE: Non-transient merge error for {target_path}: {e}")
                raise

            if attempt == max_attempts:
                logger.error(f"FAILURE: Merge failed after {max_attempts} attempts for {target_path}: {e}")
                raise

            sleep_seconds = min(8.0, (2 ** (attempt - 1)) + random.uniform(0.1, 0.8))
            logger.warning(
                f"Retry on {target_path}, "
                f"attempt {attempt}/{max_attempts}: {e}. "
                f"Sleeping {sleep_seconds:.2f}s before retry."
            )
            time.sleep(sleep_seconds)
