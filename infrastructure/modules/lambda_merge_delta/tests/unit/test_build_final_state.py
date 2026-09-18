# test_build_final_state.py

import pandas as pd
import pandas.testing as pdt
import pytest
import sys
sys.path.insert(0, "../../src")
from cdc import build_final_state

def test_returns_last_event_per_primary_key_and_preserves_group_order():
    df = pd.DataFrame(
        [
            {"id": 2, "op": "I", "name": "second"},
            {"id": 1, "op": "I", "name": "first"},
            {"id": 2, "op": "U", "name": "second updated"},
        ]
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = pd.DataFrame(
        [
            {"id": 2, "op": "U", "name": "second updated"},
            {"id": 1, "op": "I", "name": "first"},
        ]
    )

    pdt.assert_frame_equal(actual, expected)


def test_update_merges_latest_non_null_value_for_each_payload_column():
    df = pd.DataFrame(
        [
            {
                "id": 101,
                "op": "I",
                "name": "Ada",
                "email": "ada@example.com",
                "status": "active",
            },
            {
                "id": 101,
                "op": "U",
                "name": None,
                "email": "ada.lovelace@example.com",
                "status": None,
            },
            {
                "id": 101,
                "op": "U",
                "name": "Ada Lovelace",
                "email": None,
                "status": "verified",
            },
        ]
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = pd.DataFrame(
        [
            {
                "id": 101,
                "op": "U",
                "name": "Ada Lovelace",
                "email": "ada.lovelace@example.com",
                "status": "verified",
            }
        ]
    )

    pdt.assert_frame_equal(actual, expected)


def test_non_null_value_from_initial_insert_is_retained_when_update_is_null():
    df = pd.DataFrame(
        [
            {"id": 1, "op": "I", "account_type": "premium", "score": 10.0},
            {"id": 1, "op": "U", "account_type": None, "score": None},
        ]
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = pd.DataFrame(
        [{"id": 1, "op": "U", "account_type": "premium", "score": 10.0}]
    )

    pdt.assert_frame_equal(actual, expected)


def test_explicit_delete_uses_last_row_without_backfilling_payload_columns():
    df = pd.DataFrame(
        [
            {"id": 7, "op": "I", "name": "to be deleted", "region": "eu"},
            {"id": 7, "op": "U", "name": "renamed", "region": None},
            {"id": 7, "op": "D", "name": None, "region": None},
        ]
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = df.iloc[[-1]].reset_index(drop=True)

    pdt.assert_frame_equal(actual, expected)


def test_delete_followed_by_reinsert_returns_reinserted_state():
    df = pd.DataFrame(
        [
            {"id": 7, "op": "I", "name": "old value", "amount": 1},
            {"id": 7, "op": "D", "name": None, "amount": None},
            {"id": 7, "op": "I", "name": "new value", "amount": 2},
        ]
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = pd.DataFrame(
        [{"id": 7, "op": "I", "name": "new value", "amount": 2}]
    )

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_build_final_state_preserves_input_dtypes():
    df = pd.DataFrame(
        {
            "id": pd.Series([7, 7, 7], dtype="Int64"),
            "op": pd.Series(["I", "D", "I"], dtype="string"),
            "name": pd.Series(
                ["old value", pd.NA, "new value"],
                dtype="string",
            ),
            "amount": pd.Series([1, pd.NA, 2], dtype="Int64"),
        }
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = pd.DataFrame(
        {
            "id": pd.Series([7], dtype="Int64"),
            "op": pd.Series(["I"], dtype="string"),
            "name": pd.Series(["new value"], dtype="string"),
            "amount": pd.Series([2], dtype="Int64"),
        }
    )

    pdt.assert_frame_equal(actual, expected)

    pdt.assert_series_equal(actual.dtypes, df.dtypes)


def test_all_null_payload_values_remain_none_for_non_delete_event():
    df = pd.DataFrame(
        [
            {"id": 9, "op": "I", "name": None, "amount": None},
            {"id": 9, "op": "U", "name": None, "amount": None},
        ]
    )

    actual = build_final_state(df, pk_cols=["id"])

    expected = pd.DataFrame(
        [{"id": 9, "op": "U", "name": pd.NA, "amount": pd.NA}]
    )

    pdt.assert_frame_equal(actual, expected, check_dtype=False)

def test_supports_composite_primary_keys():
    df = pd.DataFrame(
        [
            {"tenant_id": "a", "order_id": 1, "op": "I", "quantity": 1},
            {"tenant_id": "a", "order_id": 2, "op": "I", "quantity": 2},
            {"tenant_id": "a", "order_id": 1, "op": "U", "quantity": 3},
            {"tenant_id": "b", "order_id": 1, "op": "I", "quantity": 4},
        ]
    )

    actual = build_final_state(
        df,
        pk_cols=["tenant_id", "order_id"],
    )

    expected = pd.DataFrame(
        [
            {"tenant_id": "a", "order_id": 1, "op": "U", "quantity": 3},
            {"tenant_id": "a", "order_id": 2, "op": "I", "quantity": 2},
            {"tenant_id": "b", "order_id": 1, "op": "I", "quantity": 4},
        ]
    )

    pdt.assert_frame_equal(actual, expected)

def test_does_not_mutate_the_input_dataframe():
    df = pd.DataFrame(
        [
            {"id": 1, "op": "I", "name": "before"},
            {"id": 1, "op": "U", "name": None},
        ]
    )
    original = df.copy(deep=True)

    build_final_state(df, pk_cols=["id"])

    pdt.assert_frame_equal(df, original)


def test_empty_input_returns_an_empty_dataframe():
    df = pd.DataFrame(columns=["id", "op", "name"])

    actual = build_final_state(df, pk_cols=["id"])

    assert actual.empty
    assert list(actual.columns) == ["id", "op", "name"]