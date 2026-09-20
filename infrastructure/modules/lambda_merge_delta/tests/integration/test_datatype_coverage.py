# datatype_coverage.py

import sys
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from datetime import date, datetime, time, timezone
from decimal import Decimal
# datatypes coverage based on check from checks/datatype_coverage.py

def get_row(table: pa.Table, **conditions):
    mask = None

    for column, value in conditions.items():
        condition = pc.equal(
            table[column],
            pa.scalar(value, type=table[column].type),
        )

        mask = condition if mask is None else pc.and_(mask, condition)

    filtered = table.filter(mask)

    assert filtered.num_rows == 1

    return {
        column: filtered[column][0].as_py()
        for column in filtered.column_names
    }

def test_all_supported_datatypes(
    monkeypatch,
    mocked_aws,
    create_delta_table,
    upload_parquet,
    create_lambda_event,
    run_lambda,
):
    schema = pa.schema([
        # Primary key / CDC columns
        pa.field("id1", pa.string()),
        pa.field("id2", pa.string()),
        pa.field("op", pa.string()),
        pa.field("optime", pa.timestamp("us")),

        # BINARY
        pa.field("binary_col", pa.binary()),

        # DATE / TIMESTAMP / TIME32
        pa.field("date_col", pa.date32()),
        # pa.field("time32_s", pa.time32("s")),
        # pa.field("time32_ms", pa.time32("ms")),
        pa.field("timestamp_s", pa.timestamp("s")),
        pa.field("timestamp_ms", pa.timestamp("ms")),
        pa.field("timestamp_us", pa.timestamp("us")),
        pa.field("timestamp_ntz", pa.timestamp("us")),
        pa.field("timestamp_utc", pa.timestamp("us", tz="UTC")),

        # INT1 / INT2 / INT4 / INT8
        pa.field("int8_col", pa.int8()),
        pa.field("int16_col", pa.int16()),
        pa.field("int32_col", pa.int32()),
        pa.field("int64_col", pa.int64()),

        # DECIMAL
        pa.field("decimal_col", pa.decimal128(38, 18)),

        # REAL4 / REAL8
        pa.field("float32_col", pa.float32()),
        pa.field("float64_col", pa.float64()),

        # STRING / WSTRING / NCLOB / CLOB
        pa.field("string_col", pa.string()),
        pa.field("wstring_col", pa.string()),
        pa.field("nclob_col", pa.string()),
        pa.field("clob_col", pa.string()),

        # BOOLEAN
        pa.field("bool_col", pa.bool_()),
    ])

    initial_table = pa.Table.from_pylist(
        [
            {
                "id1": "ROW001",
                "id2": "ROW001",
                "op": "I",
                "optime": datetime(2026, 9, 18, 9, 0, 0, 123456),

                "binary_col": b"\x00\x01\x02",

                "date_col": date(2026, 9, 18),
                # "time32_s": time(       #Exception: External error: Schema error: Invalid data type for Delta Lake: Time32(s)
                #     12, 34, 56
                # ),
                # "time32_ms": time(
                #     12, 34, 56, 123000
                # ),
                "timestamp_s": datetime(
                    2026, 9, 20,
                    12, 34, 56
                ),
                "timestamp_ms": datetime(
                    2026, 9, 20,
                    12, 34, 56, 123000
                ),
                "timestamp_us": datetime(
                    2026, 9, 20,
                    12, 34, 56, 123456
                ),
                "timestamp_ntz": datetime(
                    2026, 9, 20, 12, 34, 56, 123456
                ),
                "timestamp_utc": datetime(
                    2026, 9, 20, 12, 34, 56, 123456,
                    tzinfo=timezone.utc,
                ),

                "int8_col": -100,
                "int16_col": -30_000,
                "int32_col": -2_000_000_000,
                "int64_col": -9_000_000_000_000_000_000,

                "decimal_col": Decimal(
                    "12345678901234567890.123456789012345678"
                ),

                "float32_col": 123.5,
                "float64_col": 123456789.123456,

                "string_col": "normal string",
                "wstring_col": "Zażółć gęślą jaźń 日本語",
                "nclob_col": "NCLOB value with Unicode: ąćęłńóśźż",
                "clob_col": "CLOB value",

                "bool_col": True,
            }
        ],
        schema=schema,
    )

    delta_path = create_delta_table(initial_table)

    update_table = pa.Table.from_pylist(
        [
            {
                "id1": "ROW001",
                "id2": "ROW001",
                "op": "U",
                "optime": datetime(2026, 9, 18, 10, 0, 0, 654321),

                "binary_col": b"\xAA\xBB\xCC",

                "date_col": date(2025, 12, 31),
                # "time32_s": time(       #Exception: External error: Schema error: Invalid data type for Delta Lake: Time32(s)
                #     11, 34, 56
                # ),
                # "time32_ms": time(
                #     11, 34, 56, 123000
                # ),
                "timestamp_s": datetime(
                    2025, 9, 20,
                    12, 34, 56
                ),
                "timestamp_ms": datetime(
                    2025, 9, 20,
                    12, 34, 56, 123000
                ),
                "timestamp_us": datetime(
                    2025, 9, 20,
                    12, 34, 56, 123456
                ),
                "timestamp_ntz": datetime(
                    2025, 9, 20, 12, 34, 56, 123456
                ),
                "timestamp_utc": datetime(
                    2025, 9, 20, 12, 34, 56, 123456,
                    tzinfo=timezone.utc,
                ),

                # boundaries
                "int8_col": 127,
                "int16_col": 32_767,
                "int32_col": 2_147_483_647,
                "int64_col": 9_223_372_036_854_775_807,

                "decimal_col": Decimal(
                    "99999999999999999999.999999999999999999"
                ),

                "float32_col": 12345.5,
                "float64_col": 123456789012345.125,

                "string_col": "updated string",
                "wstring_col": "Polski: ąęćłńóśźż 日本語",
                "nclob_col": "Updated NCLOB value",
                "clob_col": "Updated CLOB value",

                "bool_col": False,
            },

            {
                "id1": "ROW002",
                "id2": "ROW002",
                "op": "I",
                "optime": datetime(2026, 9, 18, 10, 1, 0, 111111),

                "binary_col": b"\x01\x02",

                "date_col": date(2000, 1, 1),
                # "time32_s": time(     #Exception: External error: Schema error: Invalid data type for Delta Lake: Time32(s)
                #     10, 34, 56
                # ),
                # "time32_ms": time(
                #     10, 34, 56, 123000
                # ),
                "timestamp_s": datetime(
                    2020, 9, 20,
                    12, 34, 56
                ),
                "timestamp_ms": datetime(
                    2020, 9, 20,
                    12, 34, 56, 123000
                ),
                "timestamp_us": datetime(
                    2020, 9, 20,
                    12, 34, 56, 123456
                ),
                "timestamp_ns": datetime(
                    2020, 9, 20,
                    12, 34, 56, 123456
                ),
                "timestamp_ntz": datetime(
                    2020, 9, 20, 12, 34, 56, 123456
                ),
                "timestamp_utc": datetime(
                    2020, 9, 20, 12, 34, 56, 123456,
                    tzinfo=timezone.utc,
                ),

                "int8_col": -128,
                "int16_col": -32_768,
                "int32_col": -2_147_483_648,
                "int64_col": -9_223_372_036_854_775_808,

                "decimal_col": Decimal(
                    "-99999999999999999999.999999999999999999"
                ),

                "float32_col": -42.25,
                "float64_col": -987654321.125,

                "string_col": "",
                "wstring_col": "Unicode ✓",
                "nclob_col": "NCLOB",
                "clob_col": "CLOB",

                "bool_col": True,
            },
        ],
        schema=schema,
    )

    key = upload_parquet(update_table)

    event = create_lambda_event(key)

    result = run_lambda(event, delta_path)

    assert result == {
        "statusCode": 200,
        "processed": 1,
    }

    result_table = DeltaTable(str(delta_path)).to_pyarrow_table()
    assert result_table.num_rows == 2

    # ---------------------------------------------------------
    # Check resulting Delta schema
    # ---------------------------------------------------------
    result_schema = result_table.schema

    assert result_schema.field("binary_col").type == pa.binary()

    assert result_schema.field("date_col").type == pa.date32()
    # assert result_schema.field("time32_s").type == pa.time32("s")     #Exception: External error: Schema error: Invalid data type for Delta Lake: Time32(s)
    # assert result_schema.field("time32_ms").type == pa.time32("ms")
    assert result_schema.field("timestamp_s").type == pa.timestamp("us") # auto convert to us
    assert result_schema.field("timestamp_ms").type == pa.timestamp("us") # auto convert to us
    assert result_schema.field("timestamp_us").type == pa.timestamp("us")
    assert result_schema.field("timestamp_ntz").type == pa.timestamp("us")
    assert result_schema.field("timestamp_utc").type == pa.timestamp("us", tz="UTC")

    assert result_schema.field("int8_col").type == pa.int8()
    assert result_schema.field("int16_col").type == pa.int16()
    assert result_schema.field("int32_col").type == pa.int32()
    assert result_schema.field("int64_col").type == pa.int64()

    assert result_schema.field("decimal_col").type == pa.decimal128(38,18)

    assert result_schema.field("float32_col").type == pa.float32()
    assert result_schema.field("float64_col").type == pa.float64()

    assert result_schema.field("string_col").type == pa.string()
    assert result_schema.field("wstring_col").type == pa.string()
    assert result_schema.field("nclob_col").type == pa.string()
    assert result_schema.field("clob_col").type == pa.string()

    assert result_schema.field("bool_col").type == pa.bool_()

    # ---------------------------------------------------------
    # Check UPDATE row
    # ---------------------------------------------------------

    row1 = get_row(result_table, id1="ROW001")

    assert row1["optime"] == datetime(
        2026,
        9,
        18,
        10,
        0,
        0,
        654321,
    )

    assert row1["binary_col"] == b"\xAA\xBB\xCC"

    assert row1["date_col"] == date(2025, 12, 31)
    # assert row1["time32_s"] == time(11, 34, 56)           #Exception: External error: Schema error: Invalid data type for Delta Lake: Time32(s)
    # assert row1["time32_ms"] == time(11, 34, 56, 123000)
    assert row1["timestamp_s"] == datetime(
        2025, 9, 20, 12, 34, 56
    )
    assert row1["timestamp_ms"] == datetime(
        2025, 9, 20, 12, 34, 56, 123000
    )
    assert row1["timestamp_us"] == datetime(
        2025, 9, 20, 12, 34, 56, 123456
    )
    assert row1["timestamp_ntz"] == datetime(
        2025, 9, 20, 12, 34, 56, 123456
    )
    assert row1["timestamp_utc"] == datetime(
        2025, 9, 20, 12, 34, 56, 123456,
        tzinfo=timezone.utc,
    )

    assert row1["int8_col"] == 127
    assert row1["int16_col"] == 32_767
    assert row1["int32_col"] == 2_147_483_647
    assert row1["int64_col"] == 9_223_372_036_854_775_807

    assert row1["decimal_col"] == Decimal(
        "99999999999999999999.999999999999999999"
    )

    assert row1["float32_col"] == 12345.5
    assert row1["float64_col"] == 123456789012345.125

    assert row1["string_col"] == "updated string"
    assert row1["wstring_col"] == "Polski: ąęćłńóśźż 日本語"
    assert row1["nclob_col"] == "Updated NCLOB value"
    assert row1["clob_col"] == "Updated CLOB value"

    assert row1["bool_col"] is False

    # ---------------------------------------------------------
    # Check INSERT row
    # ---------------------------------------------------------
    row2 = get_row(result_table, id1="ROW002")

    assert row2["int8_col"] == -128
    assert row2["int16_col"] == -32_768
    assert row2["int32_col"] == -2_147_483_648
    assert row2["int64_col"] == -9_223_372_036_854_775_808

    assert row2["decimal_col"] == Decimal(
        "-99999999999999999999.999999999999999999"
    )

    assert row2["wstring_col"] == "Unicode ✓"

    assert row2["bool_col"] is True

    # ---------------------------------------------------------
    # Audit
    # ---------------------------------------------------------
    response = mocked_aws["audit_table"].scan()
    assert response["Count"] == 1
    audit = response["Items"][0]
    assert audit["status"] == "SUCCEEDED"
    assert audit["input_rows"] == 2
    assert audit["final_state_rows"] == 2
    assert audit["num_target_rows_inserted"] == 1
    assert audit["num_target_rows_updated"] == 1