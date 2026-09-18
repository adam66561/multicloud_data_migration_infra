# test_parse_source_location.py
import sys
sys.path.insert(0, "../../src")
from cdc import parse_source_location
import pytest

def test_parse_when_three_date_subfolder():
    class Settings:
        date_partition_subfolder_count = 3
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_pasx_cdc": "prefix3/mes_bln_pasx",
    }
    settings = Settings()
    key = "prefix1/prefix2/mes_bln_pasx_cdc/table_name/2024/06/01/data.parquet"
    target_prefix, schema_name, table_name = parse_source_location(key, settings)
    assert target_prefix == "prefix3/mes_bln_pasx"
    assert schema_name == "mes_bln_pasx"
    assert table_name == "table_name"

def test_parse_when_one_date_subfolder():
    class Settings:
        date_partition_subfolder_count = 1
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_pasx_cdc": "prefix3/mes_bln_pasx",
    }
    settings = Settings()
    key = "prefix1/prefix2/mes_bln_pasx_cdc/table_name/20260801/data.parquet"
    target_prefix, schema_name, table_name = parse_source_location(key, settings)
    assert target_prefix == "prefix3/mes_bln_pasx"
    assert schema_name == "mes_bln_pasx"
    assert table_name == "table_name"

def test_parse_when_two_date_subfolder():
    class Settings:
        date_partition_subfolder_count = 2
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_pasx_cdc": "prefix3/mes_bln_pasx",
    }
    settings = Settings()
    key = "prefix1/prefix2/mes_bln_pasx_cdc/table_name/202608/01/data.parquet"
    target_prefix, schema_name, table_name = parse_source_location(key, settings)
    assert target_prefix == "prefix3/mes_bln_pasx"
    assert schema_name == "mes_bln_pasx"
    assert table_name == "table_name"

def test_parse_when_no_prefix():
    class Settings:
        date_partition_subfolder_count = 1
        prefix_mapping= {
        "mes_bln_pasx_cdc": "mes_bln_pasx",
    }
    settings = Settings()
    key = "mes_bln_pasx_cdc/table_name/20260801/data.parquet"
    target_prefix, schema_name, table_name = parse_source_location(key, settings)
    assert target_prefix == "mes_bln_pasx"
    assert schema_name == "mes_bln_pasx"
    assert table_name == "table_name"

def test_parse_partition_less_than_zero():
    class Settings:
        date_partition_subfolder_count = -1
        prefix_mapping= {
        "mes_bln_pasx_cdc": "mes_bln_pasx",
    }
    settings = Settings()
    key = "mes_bln_pasx_cdc/table_name/20260801/data.parquet"
    with pytest.raises(ValueError, match="date_partition_subfolder_count must be zero or greater."):
        parse_source_location(key, settings)

def test_parse_when_minimum_key_parts_not_met():
    class Settings:
        date_partition_subfolder_count = 1
        prefix_mapping= {
        "mes_bln_pasx_cdc": "mes_bln_pasx",
    }
    settings = Settings()
    key = "table_name/2025/data.parquet"
    with pytest.raises(RuntimeError, match="CDC S3 key has fewer components than expected. .*"):
        parse_source_location(key, settings)

def test_parse_when_no_prefix_mapping():
    class Settings:
        date_partition_subfolder_count = 1
        prefix_mapping= {
        "mes_bln_pasx_cdc_other": "mes_bln_pasx",
    }
    settings = Settings()
    key = "mes_bln_pasx_cdc/table_name/2025/data.parquet"
    with pytest.raises(RuntimeError, match="No target prefix configured .*"):
        parse_source_location(key, settings)