from datetime import date, datetime, timezone
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs

TABLE_URI_CDC = "s3://dev-multicloud-lambda-tests/cdc/pasx/batchrecord/2025/parquet_1.parquet"

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

cdc_table = pa.Table.from_pylist(
    [
        {
            "id1": "ROW001",
            "id2": "ROW001",
            "op": "U",

            "optime": datetime(
                2027, 9, 18,
                9, 0, 0, 123456,
            ),

            "binary_col": b"\x00\x02\x03",

            "date_col": date(
                2027, 9, 18,
            ),

            "timestamp_s": datetime(
                2027, 9, 20,
                12, 34, 56,
            ),

            "timestamp_ms": datetime(
                2027, 9, 20,
                12, 34, 56, 123000,
            ),

            "timestamp_us": datetime(
                2027, 9, 20,
                12, 34, 56, 123456,
            ),

            "timestamp_ns": datetime(
                2027, 9, 20,
                12, 34, 56, 123456,
            ),

            "timestamp_ntz": datetime(
                2027, 9, 20,
                12, 34, 56, 123456,
            ),

            "timestamp_utc": datetime(
                2027, 9, 20,
                12, 34, 56, 123456,
                tzinfo=timezone.utc,
            ),

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

            "bool_col": True,
        }
    ],
    schema=schema,
)

def main():
    print("Target Parquet file:")
    print(TABLE_URI_CDC)

    print("\nInput Arrow schema:")
    print(cdc_table.schema)

    filesystem, path = fs.FileSystem.from_uri(TABLE_URI_CDC)

    pq.write_table(
        cdc_table,
        path,
        filesystem=filesystem,
    )

    print("\nParquet file created successfully.")

    result = pq.read_table(
        path,
        filesystem=filesystem,
    )

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