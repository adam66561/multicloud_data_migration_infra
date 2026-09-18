# test_lambda_failure.py

import sys
import pandas as pd
from deltalake import DeltaTable
import pytest

def test_missing_primary_key_column_fails_without_modifying_delta(
    monkeypatch,
    mocked_aws,
    create_delta_table,
    upload_parquet,
    create_lambda_event,
    run_lambda,
):
    delta_path = create_delta_table([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 10,
            "op": "I",
            "optime": "2026-09-18T10:00:00",
        }
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "quantity": 20,
            "op": "U",
            "optime": "2026-09-18T11:00:00",
        }
    ])

    event = create_lambda_event(key)

    before_df = DeltaTable(str(delta_path)).to_pandas()

    with pytest.raises(RuntimeError, match="Missing required columns"):
        run_lambda(event, delta_path)

    after_df = DeltaTable(str(delta_path)).to_pandas()

    pd.testing.assert_frame_equal(
        before_df.sort_index(axis=1),
        after_df.sort_index(axis=1),
    )

    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 0

def test_unsupported_operation_fails_without_modifying_delta(
    monkeypatch,
    mocked_aws,
    create_delta_table,
    upload_parquet,
    create_lambda_event,
    run_lambda,
):
    delta_path = create_delta_table([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 10,
            "op": "I",
            "optime": "2026-09-18T10:00:00",
        }
    ])
    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 20,
            "op": "X",
            "optime": "2026-09-18T11:00:00",
        }
    ])

    event = create_lambda_event(key)

    with pytest.raises(RuntimeError, match="Unsupported operations found"):
        run_lambda(event, delta_path)


def test_missing_delta_table_fails(
    tmp_path,
    upload_parquet,
    create_lambda_event,
    run_lambda,
    mocked_aws,
):
    missing_delta_path = tmp_path / "does-not-exist"

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 20,
            "op": "U",
            "optime": "2026-09-18T11:00:00",
        }
    ])

    event = create_lambda_event(key)

    with pytest.raises(RuntimeError, match="Delta table does not exist"):
        run_lambda(event, missing_delta_path)

    assert mocked_aws["audit_table"].scan()["Count"] == 0


def test_missing_s3_object_is_skipped(
    mocked_aws,
    create_delta_table,
    create_lambda_event,
    run_lambda,
):
    delta_path = create_delta_table([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 10,
            "op": "I",
            "optime": "2026-09-18T10:00:00",
        }
    ])

    missing_key = ("schema1/table1/2026/09/18/does-not-exist.parquet")

    event = create_lambda_event(missing_key)

    before_df = DeltaTable(str(delta_path)).to_pandas().sort_values(["id1", "id2"]).reset_index(drop=True)

    result = run_lambda(event, delta_path)

    assert result == {"statusCode": 200, "processed": 1}

    after_df = (DeltaTable(str(delta_path)).to_pandas().sort_values(["id1", "id2"]).reset_index(drop=True))

    pd.testing.assert_frame_equal(before_df, after_df)

    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 0


def test_incompatible_delta_schema_fails_without_modifying_table(
    mocked_aws,
    create_delta_table,
    upload_parquet,
    create_lambda_event,
    run_lambda,
):
    delta_path = create_delta_table([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 10,
            "op": "I",
            "optime": "2026-09-18T10:00:00",
        }
    ])

    before_df = DeltaTable(str(delta_path)).to_pandas().sort_values(["id1", "id2"]).reset_index(drop=True)

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": "this-is-not-an-integer",
            "op": "U",
            "optime": "2026-09-18T11:00:00",
        }
    ])

    event = create_lambda_event(key)

    with pytest.raises(Exception):
        run_lambda(event, delta_path)

    after_df = DeltaTable(str(delta_path)).to_pandas().sort_values(["id1", "id2"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(before_df, after_df)

    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 0