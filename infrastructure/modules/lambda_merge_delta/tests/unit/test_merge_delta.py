# test_merge_delta.py

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
import sys
sys.path.insert(0, "../../src")
from delta_merge import merge_once, merge_with_retry

#also verifies schema evolution
def test_merge_once_builds_expected_delta_merge():
    df = pd.DataFrame(
        {
            "id": [1],
            "country": ["DE"],
            "name": ["Alice"],
            "new_attribute": ["x"],
            "op": ["U"],
            "optime": ["2026-09-18T10:00:00Z"],
        }
    )

    merger = MagicMock()
    merger.when_matched_delete.return_value = merger
    merger.when_matched_update.return_value = merger
    merger.when_not_matched_insert.return_value = merger
    merger.execute.return_value = {
        "num_target_rows_updated": 1,
        "num_target_rows_inserted": 0,
    }

    delta_table = MagicMock()
    delta_table.schema.return_value.fields = [
        MagicMock(name="id"),
        MagicMock(name="country"),
        MagicMock(name="name"),
        MagicMock(name="op"),
        MagicMock(name="optime"),
    ]

    delta_table.schema.return_value.fields = [
        type("Field", (), {"name": col})()
        for col in ["id", "country", "name", "op", "optime"]
    ]
    delta_table.merge.return_value = merger

    with patch("delta_merge.DeltaTable", return_value=delta_table):
        metrics = merge_once(
            df=df,
            target_path="s3://bucket/table",
            pk_cols=["id", "country"],
        )

    assert metrics == {
        "num_target_rows_updated": 1,
        "num_target_rows_inserted": 0,
    }

    delta_table.merge.assert_called_once()
    merge_kwargs = delta_table.merge.call_args.kwargs

    assert merge_kwargs["predicate"] == (
        "target.`id` = source.`id` "
        "AND target.`country` = source.`country`"
    )
    assert merge_kwargs["source_alias"] == "source"
    assert merge_kwargs["target_alias"] == "target"
    assert merge_kwargs["merge_schema"] is True

    merger.when_matched_delete.assert_called_once_with(
        predicate="source.op = 'D' AND source.optime >= target.optime"
    )

    merger.when_matched_update.assert_called_once_with(
        predicate="source.op IN ('I', 'U') AND source.optime >= target.optime",
        updates={
            "name": "COALESCE(source.`name`, target.`name`)",
            "new_attribute": "source.`new_attribute`",
            "op": "COALESCE(source.`op`, target.`op`)",
            "optime": "COALESCE(source.`optime`, target.`optime`)",
        },
    )

    merger.when_not_matched_insert.assert_called_once_with(
        predicate="source.op IN ('I', 'U')",
        updates={
            "id": "source.`id`",
            "country": "source.`country`",
            "name": "source.`name`",
            "new_attribute": "source.`new_attribute`",
            "op": "source.`op`",
            "optime": "source.`optime`",
        },
    )

def test_merge_with_retry_returns_on_first_success():
    df = pd.DataFrame({"id": [1]})
    expected_metrics = {"num_target_rows_inserted": 1}

    with (
        patch("delta_merge.merge_once", return_value=expected_metrics) as mock_merge,
        patch("delta_merge.time.sleep") as mock_sleep,
    ):
        metrics, attempt = merge_with_retry(
            df=df,
            target_path="s3://bucket/table",
            pk_cols=["id"],
        )

    assert metrics == expected_metrics
    assert attempt == 1
    mock_merge.assert_called_once_with(
        df=df,
        target_path="s3://bucket/table",
        pk_cols=["id"],
    )
    mock_sleep.assert_not_called()

def test_merge_with_retry_retries_then_succeeds():
    df = pd.DataFrame({"id": [1]})
    expected_metrics = {"num_target_rows_updated": 1}

    with (
        patch(
            "delta_merge.merge_once",
            side_effect=[RuntimeError("conflict"), expected_metrics],
        ) as mock_merge,
        patch("delta_merge.time.sleep") as mock_sleep,
        patch("delta_merge.random.uniform", return_value=0.25),
    ):
        metrics, attempt = merge_with_retry(
            df=df,
            target_path="s3://bucket/table",
            pk_cols=["id"],
            max_attempts=3,
        )

    assert metrics == expected_metrics
    assert attempt == 2
    assert mock_merge.call_count == 2
    mock_sleep.assert_called_once_with(1.25)


def test_merge_with_retry_raises_after_last_attempt():
    df = pd.DataFrame({"id": [1]})

    with (
        patch(
            "delta_merge.merge_once",
            side_effect=RuntimeError("persistent Delta conflict"),
        ) as mock_merge,
        patch("delta_merge.time.sleep") as mock_sleep,
        patch("delta_merge.random.uniform", return_value=0.25),
        pytest.raises(RuntimeError, match="persistent Delta conflict"),
    ):
        merge_with_retry(
            df=df,
            target_path="s3://bucket/table",
            pk_cols=["id"],
            max_attempts=3,
        )

    assert mock_merge.call_count == 3
    assert mock_sleep.call_count == 2

def test_merge_with_retry_non_transient_error():
    df = pd.DataFrame({"id": [1]})

    with (
        patch(
            "delta_merge.merge_once",
            side_effect=RuntimeError("non-transient error"),
        ) as mock_merge,
        patch("delta_merge.time.sleep") as mock_sleep,
        patch("delta_merge.random.uniform", return_value=0.25),
        pytest.raises(RuntimeError, match="non-transient error"),
    ):
        merge_with_retry(
            df=df,
            target_path="s3://bucket/table",
            pk_cols=["id"],
            max_attempts=3,
        )

    assert mock_merge.call_count == 1
    assert mock_sleep.call_count == 0