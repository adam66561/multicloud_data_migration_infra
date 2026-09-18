# test_lambda_success.py

import sys
import pandas as pd
from deltalake import DeltaTable


def test_insert_and_update(
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
            "quantity": 5,
            "op": "I",
            "optime": "2026-09-18T09:00:00",
        }
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 10,
            "op": "U",
            "optime": "2026-09-18T10:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 20,
            "op": "U",
            "optime": "2026-09-18T10:01:00",
        },
        {
            "id1": "MAT002",
            "id2": "002",
            "quantity": 30,
            "op": "I",
            "optime": "2026-09-18T10:02:00",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}


    dt = DeltaTable(str(delta_path))

    result_df = (dt.to_pandas().sort_values(["id1", "id2"]).reset_index(drop=True))

    assert len(result_df) == 2


    mat001 = result_df[result_df["id1"] == "MAT001"].iloc[0]

    assert mat001["quantity"] == 20
    assert mat001["op"] == "U"
    assert mat001["optime"] == "2026-09-18T10:01:00"


    mat002 = result_df[result_df["id1"] == "MAT002"].iloc[0]
    assert mat002["quantity"] == 30
    assert mat002["op"] == "I"


    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 1
    audit = response["Items"][0]
    assert audit["status"] == "SUCCEEDED"
    assert audit["input_rows"] == 3
    assert audit["final_state_rows"] == 2
    assert audit["num_target_rows_inserted"] == 1
    assert audit["num_target_rows_updated"] == 1


def test_insert_and_delete(
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
            "optime": "2026-09-18T09:00:00",
        },
        {
            "id1": "MAT002",
            "id2": "002",
            "quantity": 20,
            "op": "I",
            "optime": "2026-09-18T09:00:00",
        },
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 50,
            "op": "U",
            "optime": "2026-09-18T10:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": None,
            "op": "D",
            "optime": "2026-09-18T10:01:00",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}

    dt = DeltaTable(str(delta_path))
    result_df = (dt.to_pandas().sort_values(["id1", "id2"]).reset_index(drop=True))
    assert not (result_df["id1"] == "MAT001").any()
    assert (result_df["id1"] == "MAT002").any()

    response = mocked_aws["audit_table"].scan()
    audit = response["Items"][0]
    assert audit["num_target_rows_deleted"] == 1


def test_multiple_composite_primary_keys(
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
            "optime": "2026-09-18T09:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "002",
            "quantity": 20,
            "op": "I",
            "optime": "2026-09-18T09:00:00",
        },
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 100,
            "op": "U",
            "optime": "2026-09-18T10:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "002",
            "quantity": 200,
            "op": "U",
            "optime": "2026-09-18T10:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 150,
            "op": "U",
            "optime": "2026-09-18T10:01:00",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}

    result_df = (DeltaTable(str(delta_path)).to_pandas().sort_values("id2").reset_index(drop=True))
    assert len(result_df) == 2

    pk_001 = result_df[result_df["id2"] == "001"].iloc[0]
    pk_002 = result_df[result_df["id2"] == "002"].iloc[0]
    assert pk_001["quantity"] == 150
    assert pk_002["quantity"] == 200


def test_schema_evolution_adds_new_column(
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
            "optime": "2026-09-18T09:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "002",
            "quantity": 20,
            "op": "I",
            "optime": "2026-09-18T09:00:00",
        },
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 100,
            "op": "U",
            "optime": "2026-09-18T10:00:00",
            "new_column": "new_value_1",
        },
        {
            "id1": "MAT001",
            "id2": "002",
            "quantity": 200,
            "op": "U",
            "optime": "2026-09-18T10:00:00",
            "new_column2": "new_value_2",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}

    result_df = (DeltaTable(str(delta_path)).to_pandas().sort_values("id2").reset_index(drop=True))
    assert len(result_df) == 2

    pk_001 = result_df[result_df["id2"] == "001"].iloc[0]
    pk_002 = result_df[result_df["id2"] == "002"].iloc[0]
    assert pk_001["new_column"] == "new_value_1"
    assert pd.isna(pk_001["new_column2"])
    assert pk_002["new_column2"] == "new_value_2"
    assert pd.isna(pk_002["new_column"])


def test_rows_after_delete_do_no_inherit_deleted_values(
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
            "optime": "2026-09-18T09:00:00",
        }
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": None,
            "op": "D",
            "optime": "2026-09-18T10:00:00",
        },
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 200,
            "op": "I",
            "optime": "2026-09-18T10:00:00",
            "new_column": "new_value_2",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}

    result_df = (DeltaTable(str(delta_path)).to_pandas().sort_values("id2").reset_index(drop=True))
    assert len(result_df) == 1

    pk_001 = result_df[result_df["id2"] == "001"].iloc[0]
    assert pk_001["new_column"] == "new_value_2"
    assert pk_001["quantity"] == 200


def test_optime_must_be_greater_than_existing(
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
            "op": "U",
            "optime": "2026-09-18T09:00:00",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}

    result_df = (DeltaTable(str(delta_path)).to_pandas().sort_values("id2").reset_index(drop=True))
    assert len(result_df) == 1

    pk_001 = result_df[result_df["id2"] == "001"].iloc[0]
    assert pk_001["optime"] == "2026-09-18T10:00:00"
    assert pk_001["quantity"] == 10


def test_only_not_null_values(
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
            "col1": "col1_value",
            "col2": "col2_value",
            "op": "I",
            "optime": "2026-09-18T10:00:00",
        }
    ])

    key = upload_parquet([
        {
            "id1": "MAT001",
            "id2": "001",
            "quantity": 20,
            "col1": None,
            "col2": "col2_value_updated",
            "op": "U",
            "optime": "2026-09-18T11:00:00",
        },
    ])

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)
    assert result == {"statusCode": 200, "processed": 1}

    result_df = (DeltaTable(str(delta_path)).to_pandas().sort_values("id2").reset_index(drop=True))
    assert len(result_df) == 1

    pk_001 = result_df[result_df["id2"] == "001"].iloc[0]
    assert pk_001["quantity"] == 20
    assert pk_001["col1"] == "col1_value"
    assert pk_001["col2"] == "col2_value_updated"