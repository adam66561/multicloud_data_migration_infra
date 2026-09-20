# test_datatype_coverage.py
# parquet datatypes not used in postgres or oracle: UINT1 UINT2 UINT4 UINT8

#---SOURCES-----#
#Oracle  #Postgres          # AWS DMS       S3 parquet      Pyarrow                 Delta
# -------  ---------          --------      ----------      --------                -----
#                             BYTES	        BINARY          binary                  binary
#                             BLOB	        BINARY          binary                  binary
#                             DATE	        DATE32          date32                  date
#                             TIME	        TIME32          time32                  NOT SUPPORTED: need for conversion Schema error: Invalid data type for Delta Lake: Time32(s)
#                             DATETIME	    TIMESTAMP       timestamp               timestamp_ntz BELOW DETAILS
#                             INT1 2 4 8    INT8 16 32 64   INT8 16 32 64           byte, short, integer, long
#                             NUMERIC       DECIMAL         DECIMAL32/64/128/256    decimal 38,38 max BELOW DETAILS
#                             REAL4	        FLOAT           FLOAT32                 float
#                             REAL8	        DOUBLE          FLOAT64                 double
#                             STRING        STRING          STRING                  string
#                             WSTRING	    STRING          STRING                  string
#                             NCLOB	        STRING          STRING                  string
#                             CLOB	        STRING          STRING                  string
#                             BOOLEAN       BOOL            BOOL                    boolean
# AWS DMS ParquetTimestampInMillisecond=false (default)
#     -> TIMESTAMP written with microsecond precision
#     -> PyArrow timestamp[us]
#     -> supported by delta-rs
# miliseconds may not work

from deltalake.schema import PrimitiveType
import pyarrow as pa

types = [
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.float32(),
    pa.float64(),
    pa.bool_(),
    pa.string(),
    pa.binary(),
    pa.date32(),
    pa.time32('s'),
    pa.time32('ms'),
    pa.timestamp("us"),
    pa.decimal128(18, 4),
]

for arrow_type in types:
    try:
        delta_type = PrimitiveType.from_arrow(arrow_type)
        print(f"{arrow_type!s:25} -> {delta_type}")
    except Exception as e:
        print(f"{arrow_type!s:25} -> NOT SUPPORTED: {e}")

print("")
print("")
print("")
print("")


#decimal precision check

for precision, scale in [
    (38, 0),
    (38, 1),
    (38, 18),
    (38, 37),
    (38, 38),
]:
    arrow_type = pa.decimal128(precision, scale)
    delta_type = PrimitiveType.from_arrow(arrow_type)

    print(
        f"{arrow_type} -> {delta_type}"
    )

tests = [
    pa.decimal128(38, 38),
    pa.decimal128(38, 18),
    pa.decimal256(39, 18),
    pa.decimal256(50, 10),
    pa.decimal256(76, 38),
]

for arrow_type in tests:
    try:
        delta_type = PrimitiveType.from_arrow(arrow_type)
        print(f"{arrow_type} -> {delta_type}")
    except Exception as e:
        print(f"{arrow_type} -> NOT SUPPORTED: {e}")
#OUTPUT
# decimal128(38, 0) -> PrimitiveType("decimal(38,0)")
# decimal128(38, 1) -> PrimitiveType("decimal(38,1)")
# decimal128(38, 18) -> PrimitiveType("decimal(38,18)")
# decimal128(38, 37) -> PrimitiveType("decimal(38,37)")
# decimal128(38, 38) -> PrimitiveType("decimal(38,38)")
# decimal128(38, 38) -> PrimitiveType("decimal(38,38)")
# decimal128(38, 18) -> PrimitiveType("decimal(38,18)")
# decimal256(39, 18) -> NOT SUPPORTED: Schema error: Invalid data type for Delta Lake: Decimal256(39, 18)
# decimal256(50, 10) -> NOT SUPPORTED: Schema error: Invalid data type for Delta Lake: Decimal256(50, 10)
# decimal256(76, 38) -> NOT SUPPORTED: Schema error: Invalid data type for Delta Lake: Decimal256(76, 38)
#MAX: 38. 38

print("")
print("")
print("")
print("")

#timstamp check
timestamp_tests = [
    pa.timestamp("us"),
    pa.timestamp("us", tz="UTC"),
    pa.timestamp("us", tz="Europe/Warsaw"),
    pa.timestamp("ns"),
    pa.timestamp("ns", tz="UTC"),
]

for arrow_type in timestamp_tests:
    try:
        print(
            f"{arrow_type!s:35} -> "
            f"{PrimitiveType.from_arrow(arrow_type)}"
        )
    except Exception as e:
        print(f"{arrow_type!s:35} -> NOT SUPPORTED: {e}")


for unit in ["s", "ms", "us", "ns"]:
    arrow_type = pa.timestamp(unit)

    try:
        print(
            arrow_type,
            "->",
            PrimitiveType.from_arrow(arrow_type)
        )
    except Exception as e:
        print(
            arrow_type,
            "-> unsupported:",
            e
        )
#output:
# timestamp[s] -> unsupported: Schema error: Invalid data type for Delta Lake: Timestamp(s)
# timestamp[ms] -> unsupported: Schema error: Invalid data type for Delta Lake: Timestamp(ms)
# timestamp[us] -> PrimitiveType("timestamp_ntz")
# timestamp[ns] -> PrimitiveType("timestamp_ntz")
#implication: delta should autoconvert to 6 places precision, tested? NO
# AWS DMS ParquetTimestampInMillisecond=false (default)
#     -> TIMESTAMP written with microsecond precision
#     -> PyArrow timestamp[us]
#     -> supported by delta-rs
# miliseconds may not work
