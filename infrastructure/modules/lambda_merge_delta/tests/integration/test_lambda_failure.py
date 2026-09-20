# test_lambda_failure.py

import sys
import pyarrow as pa
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

    before = DeltaTable(str(delta_path)).to_pyarrow_table()

    with pytest.raises(RuntimeError, match="Missing required columns"):
        run_lambda(event, delta_path)

    after = DeltaTable(str(delta_path)).to_pyarrow_table()

    assert before.equals(after)

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

    before = DeltaTable(str(delta_path)).to_pyarrow_table()

    with pytest.raises(RuntimeError, match="Unsupported operations found"):
        run_lambda(event, delta_path)

    after = DeltaTable(str(delta_path)).to_pyarrow_table()

    assert before.equals(after)

    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 0


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

    before = DeltaTable(str(delta_path)).to_pyarrow_table()

    result = run_lambda(event, delta_path)

    assert result == {"statusCode": 200, "processed": 1}

    after = DeltaTable(str(delta_path)).to_pyarrow_table()

    assert before.equals(after)

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

    before = DeltaTable(str(delta_path)).to_pyarrow_table()

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

    after = DeltaTable(str(delta_path)).to_pyarrow_table()

    assert before.equals(after)

    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 0