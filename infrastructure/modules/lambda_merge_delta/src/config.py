# config.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    source_bucket: str
    target_bucket: str
    config_bucket: str
    config_key: str
    audit_logs: bool
    audit_table_name: str
    prefix_mapping: dict[str, str]
    date_partition_subfolder_count: int

def load_settings() -> Settings:
    try:
        return Settings(
            source_bucket=os.environ["S3_SOURCE_BUCKET"],
            target_bucket=os.environ["S3_TARGET_BUCKET"],
            config_bucket=os.environ["S3_CONFIG_BUCKET"],
            config_key=os.environ["S3_CONFIG_KEY"],
            audit_logs=os.environ.get("AUDIT_LOGS", "false").lower() == "true",
            audit_table_name=os.environ["AUDIT_TABLE_NAME"],
            prefix_mapping=json.loads(os.environ["PREFIX_MAPPING"]),
            date_partition_subfolder_count=int(
                os.environ.get("DATE_PARTITION_SUBFOLDER_COUNT", "0")
            ),
        )
    except KeyError as exc:
        raise RuntimeError(
            f"Required environment variable is missing: {exc}"
        ) from exc
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("Invalid Lambda configuration") from exc
