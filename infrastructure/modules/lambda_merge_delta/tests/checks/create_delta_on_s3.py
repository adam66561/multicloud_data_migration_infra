from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

TABLE_URI = "s3://dev-multicloud-lambda-tests/pasx/batchrecord/"


schema = pa.schema([
    pa.field("id1", pa.string()),
    pa.field("id2", pa.string()),
    pa.field("op", pa.string()),

    # CDC timestamp
    pa.field("optime", pa.timestamp("us")),

    # BINARY / BLOB
    pa.field("binary_col", pa.binary()),

    # DATE
    pa.field("date_col", pa.date32()),

    # TIMESTAMP variants
    pa.field("timestamp_s", pa.timestamp("s")),
    pa.field("timestamp_ms", pa.timestamp("ms")),
    pa.field("timestamp_us", pa.timestamp("us")),
    pa.field("timestamp_ns", pa.timestamp("ns")),

    # Timestamp without timezone
    pa.field("timestamp_ntz", pa.timestamp("us")),

    # UTC timestamp
    pa.field(
        "timestamp_utc",
        pa.timestamp("us", tz="UTC"),
    ),

    # INT1 / INT2 / INT4 / INT8
    pa.field("int8_col", pa.int8()),
    pa.field("int16_col", pa.int16()),
    pa.field("int32_col", pa.int32()),
    pa.field("int64_col", pa.int64()),

    # NUMERIC
    pa.field(
        "decimal_col",
        pa.decimal128(38, 18),
    ),

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

            "optime": datetime(
                2026, 9, 18,
                9, 0, 0, 123456,
            ),

            "binary_col": b"\x00\x01\x02",

            "date_col": date(
                2026, 9, 18,
            ),

            "timestamp_s": datetime(
                2026, 9, 20,
                12, 34, 56,
            ),

            "timestamp_ms": datetime(
                2026, 9, 20,
                12, 34, 56, 123000,
            ),

            "timestamp_us": datetime(
                2026, 9, 20,
                12, 34, 56, 123456,
            ),

            "timestamp_ns": datetime(
                2026, 9, 20,
                12, 34, 56, 123456,
            ),

            "timestamp_ntz": datetime(
                2026, 9, 20,
                12, 34, 56, 123456,
            ),

            "timestamp_utc": datetime(
                2026, 9, 20,
                12, 34, 56, 123456,
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


def main():
    print("Target Delta table:")
    print(TABLE_URI)

    print("\nInput Arrow schema:")
    print(initial_table.schema)

    storage_options = {
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    write_deltalake(
        TABLE_URI,
        initial_table,
        mode="overwrite",
        storage_options=storage_options,
    )

    print("\nDelta table created successfully.")

    dt = DeltaTable(
        TABLE_URI,
        storage_options=storage_options,
    )

    print(f"Delta version: {dt.version()}")

    print("\nDelta schema:")
    print(dt.schema().to_arrow())

    result = dt.to_pyarrow_table()

    print("\nRead-back Arrow schema:")
    print(result.schema)

    print("\nRead-back data:")
    print(result)

    print("\nDetailed resulting types:")

    for field in result.schema:
        print(
            f"{field.name:20} -> {field.type}"
        )


if __name__ == "__main__":
    main()