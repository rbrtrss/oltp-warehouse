from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess
import os

import pyarrow.parquet as pq

BRONZE_TABLES = {
    "accounts": {
        "required_columns": {
            "account_id",
            "customer_id",
            "account_type",
            "status",
            "currency_code",
            "balance",
            "created_at",
            "updated_at",
        }
    },
    "transactions": {
        "required_columns": {
            "transaction_id",
            "account_id",
            "transaction_type",
            "amount",
            "status",
            "description",
            "created_at",
            "updated_at",
        }
    },
    "transfers": {
        "required_columns": {
            "transfer_id",
            "source_account_id",
            "destination_account_id",
            "amount",
            "status",
            "created_at",
            "updated_at",
        }
    },
    "payments": {
        "required_columns": {
            "payment_id",
            "account_id",
            "merchant_name",
            "category",
            "amount",
            "status",
            "created_at",
            "updated_at",
        }
    },
}

SILVER_MODELS = {
    "silver_accounts": {
        "path": "silver_accounts.parquet",
        "required_columns": {"account_id", "updated_at", "bronze_file_path"},
    },
    "silver_transactions": {
        "path": "silver_transactions.parquet",
        "required_columns": {"transaction_id", "updated_at", "bronze_file_path"},
    },
    "silver_transfers": {
        "path": "silver_transfers.parquet",
        "required_columns": {"transfer_id", "updated_at", "bronze_file_path"},
    },
    "silver_payments": {
        "path": "silver_payments.parquet",
        "required_columns": {"payment_id", "updated_at", "bronze_file_path"},
    },
}


class ValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ValidateConfig:
    bronze_root: Path = Path("data/raw/cdc")
    state_path: Path = Path("data/state/cdc_state.json")
    silver_root: Path = Path("data/silver")
    profiles_dir: Path = Path(".")
    project_dir: Path = Path(".")
    run_dbt_tests: bool = True


def validate_local_pipeline(config: ValidateConfig) -> dict[str, object]:
    bronze = validate_bronze_outputs(config.bronze_root, config.state_path)
    silver = validate_silver_outputs(config.silver_root)
    dbt_status = None
    if config.run_dbt_tests:
        dbt_status = run_dbt_tests(
            profiles_dir=config.profiles_dir,
            project_dir=config.project_dir,
        )
    return {
        "bronze": bronze,
        "silver": silver,
        "dbt_test": dbt_status,
    }


def validate_bronze_outputs(bronze_root: Path, state_path: Path) -> dict[str, object]:
    summaries: dict[str, dict[str, object]] = {}
    for table_name, spec in BRONZE_TABLES.items():
        table_dir = bronze_root / table_name
        if not table_dir.exists():
            raise ValidationError(f"Missing bronze directory for table '{table_name}': {table_dir}")

        parquet_files = sorted(table_dir.glob("*.parquet"))
        if not parquet_files:
            raise ValidationError(f"No parquet files found for bronze table '{table_name}'")

        total_rows = 0
        discovered_columns: set[str] = set()
        for parquet_file in parquet_files:
            parquet = pq.ParquetFile(parquet_file)
            total_rows += parquet.metadata.num_rows
            discovered_columns.update(parquet.schema.names)

        if total_rows <= 0:
            raise ValidationError(f"Bronze table '{table_name}' contains no rows")

        missing_columns = spec["required_columns"] - discovered_columns
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValidationError(f"Bronze table '{table_name}' is missing required columns: {missing}")

        summaries[table_name] = {
            "files": len(parquet_files),
            "rows": total_rows,
        }

    state = validate_watermark_state(state_path)
    return {"tables": summaries, "state": state}


def validate_watermark_state(state_path: Path) -> dict[str, str | None]:
    if not state_path.exists():
        raise ValidationError(f"Missing CDC state file: {state_path}")

    payload = json.loads(state_path.read_text())
    missing_tables = set(BRONZE_TABLES) - set(payload)
    if missing_tables:
        missing = ", ".join(sorted(missing_tables))
        raise ValidationError(f"CDC state file is missing table entries: {missing}")

    validated_state: dict[str, str | None] = {}
    for table_name in BRONZE_TABLES:
        value = payload[table_name]
        if value is not None:
            try:
                datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValidationError(
                    f"CDC watermark for table '{table_name}' is not a valid ISO timestamp: {value}"
                ) from exc
        validated_state[table_name] = value
    return validated_state


def validate_silver_outputs(silver_root: Path) -> dict[str, object]:
    summaries: dict[str, dict[str, object]] = {}
    for model_name, spec in SILVER_MODELS.items():
        output_path = silver_root / spec["path"]
        if not output_path.exists():
            raise ValidationError(f"Missing silver output for model '{model_name}': {output_path}")

        parquet = pq.ParquetFile(output_path)
        row_count = parquet.metadata.num_rows
        if row_count <= 0:
            raise ValidationError(f"Silver model '{model_name}' contains no rows")

        columns = set(parquet.schema.names)
        missing_columns = spec["required_columns"] - columns
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValidationError(f"Silver model '{model_name}' is missing required columns: {missing}")

        summaries[model_name] = {
            "rows": row_count,
            "path": str(output_path),
        }

    return {"models": summaries}


def run_dbt_tests(*, profiles_dir: Path, project_dir: Path) -> dict[str, object]:
    dbt_executable = shutil.which("dbt")
    if not dbt_executable:
        raise ValidationError("dbt is not installed. Run `uv sync` or `pip install -e .` first.")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    subprocess.run(
        [
            dbt_executable,
            "test",
            "--project-dir",
            str(project_dir),
            "--select",
            "silver",
        ],
        check=True,
        env=env,
    )
    return {"status": "passed"}
