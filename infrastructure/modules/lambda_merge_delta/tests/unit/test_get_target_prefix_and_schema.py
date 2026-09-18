# test_get_target_prefix_and_schema.py
import pytest
import sys
sys.path.insert(0, "../../src")
from cdc import get_target_prefix_and_schema

import json
import os

def test_prefix_mapping(monkeypatch):
    monkeypatch.setenv(
        "PREFIX_MAPPING",
        json.dumps({
            "mes_bln_pasx_cdc": "mes_bln_pasx",
            "mes_bln_wltuser_cdc": "mes_bln_wltuser",
        }),
    )

    prefix_mapping = json.loads(os.environ["PREFIX_MAPPING"])

    assert prefix_mapping == {
        "mes_bln_pasx_cdc": "mes_bln_pasx",
        "mes_bln_wltuser_cdc": "mes_bln_wltuser",
    }
    assert prefix_mapping["mes_bln_pasx_cdc"] == "mes_bln_pasx"

def test_get_target_prefix_and_schema_normal():
    class Settings:
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_pasx_cdc": "prefix3/mes_bln_pasx",
        "prefix1/prefix2/mes_bln_wltuser_cdc": "mes_bln_wltuser",
    }
    settings = Settings()
    source_prefix = "prefix1/prefix2/mes_bln_pasx_cdc"
    target_prefix, target_schema = get_target_prefix_and_schema(source_prefix, settings.prefix_mapping)
    assert target_prefix == "prefix3/mes_bln_pasx"
    assert target_schema == "mes_bln_pasx"

def test_get_target_prefix_and_schema_endslash():
    class Settings:
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_pasx_cdc": "prefix3/mes_bln_pasx/",
        "prefix1/prefix2/mes_bln_wltuser_cdc": "mes_bln_wltuser",
    }
    settings = Settings()
    source_prefix = "prefix1/prefix2/mes_bln_pasx_cdc"
    target_prefix, target_schema = get_target_prefix_and_schema(source_prefix, settings.prefix_mapping)
    assert target_prefix == "prefix3/mes_bln_pasx"
    assert target_schema == "mes_bln_pasx"

def test_get_target_prefix_and_schema_partition_no_target_prefix():
    class Settings:
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_pasx_cdc": "",
        "prefix1/prefix2/mes_bln_wltuser_cdc": "mes_bln_wltuser",
    }
    settings = Settings()
    source_prefix = "prefix1/prefix2/mes_bln_pasx_cdc"
    with pytest.raises(RuntimeError, match="Target prefix is empty .*"):
        get_target_prefix_and_schema(source_prefix, settings.prefix_mapping)

def test_get_target_prefix_and_schema_partition_no_target_prefix2():
    class Settings:
        prefix_mapping= {
        "prefix1/prefix2/mes_bln_wltuser_cdc": "mes_bln_wltuser",
    }
    settings = Settings()
    source_prefix = "prefix1/prefix2/mes_bln_pasx_cdc"
    with pytest.raises(RuntimeError, match="No target prefix configured .*"):
        get_target_prefix_and_schema(source_prefix, settings.prefix_mapping)