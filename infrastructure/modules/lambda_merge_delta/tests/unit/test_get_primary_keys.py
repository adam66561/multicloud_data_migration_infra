# test_get_primary_keys.py
import pytest
from unittest.mock import MagicMock, patch
import json
import sys
sys.path.insert(0, "../../src")
from cdc import get_primary_keys

PRIMARY_KEYS = {
    "schema_name.table_name": ["key1", "key2"],
    "schema_name2.table_name2": ["key3"],
    "schema_name3.table_name3": ["key4"],
    "schema_name4.table_name4": [],
    "schema_name5.table_name5": [""],
}

@patch("cdc.s3_client")
def test_get_primary_keys(mock_s3_client):
    class Settings: 
        config_bucket="test-config-bucket"
        config_key="test-config-key"
    settings = Settings()

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(PRIMARY_KEYS).encode("utf-8")))
    }

    schema_name = "schema_name"
    table_name = "table_name"

    primary_keys = get_primary_keys(schema_name, table_name, settings)

    assert primary_keys == ["key1", "key2"]
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-config-bucket",
        Key="test-config-key",
    )

@patch("cdc.s3_client")
def test_get_primary_keys_wrong_schema(mock_s3_client):
    class Settings: 
        config_bucket="test-config-bucket"
        config_key="test-config-key"
    settings = Settings()

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(PRIMARY_KEYS).encode("utf-8")))
    }

    schema_name = "schema_name_other"
    table_name = "table_name"

    with pytest.raises(RuntimeError, match="No primary key configured .*"):
        get_primary_keys(schema_name, table_name, settings)

@patch("cdc.s3_client")
def test_get_primary_keys_no_primary_key(mock_s3_client):
    class Settings: 
        config_bucket="test-config-bucket"
        config_key="test-config-key"
    settings = Settings()

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(PRIMARY_KEYS).encode("utf-8")))
    }

    schema_name = "schema_name4"
    table_name = "table_name4"

    with pytest.raises(RuntimeError, match="No primary key configured .*"):
        get_primary_keys(schema_name, table_name, settings)

@patch("cdc.s3_client")
def test_get_primary_keys_empty_primary_key(mock_s3_client):
    class Settings: 
        config_bucket="test-config-bucket"
        config_key="test-config-key"
    settings = Settings()

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(PRIMARY_KEYS).encode("utf-8")))
    }

    schema_name = "schema_name5"
    table_name = "table_name5"

    with pytest.raises(RuntimeError, match="Invalid primary key configuration .*"):
        get_primary_keys(schema_name, table_name, settings)