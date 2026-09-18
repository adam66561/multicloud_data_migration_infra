# test_datatype_coverage.py
# datatypes not used in postgres or oracle: UINT1 UINT2 UINT4 UINT8
# AWS DMS       S3 parquet
# --------      ----------
# BYTES	        BINARY
# BLOB	        BINARY
# DATE	        DATE32
# TIME	        TIME32
# DATETIME	    TIMESTAMP
# INT1	        INT8
# INT2	        INT16
# INT4	        INT32
# INT8	        INT64
# NUMERIC	    DECIMAL
# REAL4	        FLOAT
# REAL8	        DOUBLE
# STRING	    STRING
# WSTRING	    STRING
# NCLOB	        STRING
# CLOB	        STRING
# BOOLEAN	    BOOL