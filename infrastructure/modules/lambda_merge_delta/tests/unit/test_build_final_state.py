# test_build_final_state.py

import pyarrow as pa
import pytest
import sys
sys.path.insert(0, "../../src")
from cdc import build_final_state

def test_returns_last_event_per_primary_key_and_preserves_group_order():
    table = pa.table(
        {
            "id": [2, 1, 2],
            "op": ["I", "I", "U"],
            "name": ["second", "first", "second updated"],
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    expected = pa.table(
        {
            "id": [2, 1],
            "op": ["U", "I"],
            "name": ["second updated", "first"],
        }
    )

    assert actual.equals(expected)
    assert num_rows == 2


def test_update_merges_latest_non_null_value_for_each_payload_column():
    table = pa.table(
        {
            "id": [101, 101, 101],
            "op": ["I", "U", "U"],
            "name": [
                "Ada",
                None,
                "Ada Lovelace",
            ],
            "email": [
                "ada@example.com",
                "ada.lovelace@example.com",
                None,
            ],
            "status": [
                "active",
                None,
                "verified",
            ],
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    expected = pa.table(
        {
            "id": [101],
            "op": ["U"],
            "name": ["Ada Lovelace"],
            "email": ["ada.lovelace@example.com"],
            "status": ["verified"],
        }
    )

    assert actual.equals(expected)
    assert num_rows == 1


def test_non_null_value_from_initial_insert_is_retained_when_update_is_null():
    table = pa.table(
        {
            "id": [1, 1],
            "op": ["I", "U"],
            "account_type": ["premium", None],
            "score": pa.array(
                [10.0, None],
                type=pa.float64(),
            ),
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    expected = pa.table(
        {
            "id": [1],
            "op": ["U"],
            "account_type": ["premium"],
            "score": pa.array(
                [10.0],
                type=pa.float64(),
            ),
        }
    )

    assert actual.equals(expected)
    assert num_rows == 1


def test_explicit_delete_uses_last_row_without_backfilling_payload_columns():
    table = pa.table(
        {
            "id": [7, 7, 7],
            "op": ["I", "U", "D"],
            "name": [
                "to be deleted",
                "renamed",
                None,
            ],
            "region": [
                "eu",
                None,
                None,
            ],
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    expected = table.slice(2, 1)

    assert actual.equals(expected)
    assert num_rows == 1


def test_delete_followed_by_reinsert_returns_reinserted_state():
    table = pa.table(
        {
            "id": [7, 7, 7],
            "op": ["I", "D", "I"],
            "name": [
                "old value",
                None,
                "new value",
            ],
            "amount": pa.array(
                [1, None, None],
                type=pa.int64(),
            ),
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    expected = pa.table(
        {
            "id": [7],
            "op": ["I"],
            "name": ["new value"],
            "amount": pa.array(
                [None],
                type=pa.int64(),
            ),
        }
    )

    assert actual.equals(expected)
    assert num_rows == 1


def test_all_null_payload_values_remain_none_for_non_delete_event():
    table = pa.table(
        {
            "id": [9, 9],
            "op": ["I", "U"],
            "name": pa.array(
                [None, None],
                type=pa.string(),
            ),
            "amount": pa.array(
                [None, None],
                type=pa.int64(),
            ),
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    expected = pa.table(
        {
            "id": [9],
            "op": ["U"],
            "name": pa.array(
                [None],
                type=pa.string(),
            ),
            "amount": pa.array(
                [None],
                type=pa.int64(),
            ),
        }
    )

    assert actual.equals(expected)
    assert num_rows == 1

def test_supports_composite_primary_keys():
    table = pa.table(
        {
            "tenant_id": ["a", "a", "a", "b"],
            "order_id": [1, 2, 1, 1],
            "op": ["I", "I", "U", "I"],
            "quantity": [1, 2, 3, 4],
        }
    )

    actual, num_rows = build_final_state(
        table,
        pk_cols=["tenant_id", "order_id"],
    )

    expected = pa.table(
        {
            "tenant_id": ["a", "a", "b"],
            "order_id": [1, 2, 1],
            "op": ["U", "I", "I"],
            "quantity": [3, 2, 4],
        }
    )

    assert actual.equals(expected)
    assert num_rows == 3

def test_does_not_mutate_the_input_table():
    table = pa.table(
        {
            "id": [1, 1],
            "op": ["I", "U"],
            "name": ["before", None],
        }
    )

    original = table

    build_final_state(table, pk_cols=["id"])

    assert table.equals(original)


def test_empty_input_returns_an_empty_table():
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "op": pa.array([], type=pa.string()),
            "name": pa.array([], type=pa.string()),
        }
    )

    actual, num_rows = build_final_state(table, pk_cols=["id"])

    assert num_rows == 0
    assert actual.column_names == ["id", "op", "name"]
    assert actual.schema == table.schema
