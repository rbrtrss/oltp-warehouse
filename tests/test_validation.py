from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oltp_warehouse.validation import (
    ValidationError,
    validate_bronze_outputs,
    validate_local_pipeline,
    validate_silver_outputs,
)


BRONZE_ROWS = {
    "accounts": {
        "account_id": [1000],
        "customer_id": ["CUST-001000"],
        "account_type": ["checking"],
        "status": ["active"],
        "currency_code": ["USD"],
        "balance": [100.0],
        "created_at": ["2026-01-01T09:00:00"],
        "updated_at": ["2026-01-01T09:00:00"],
    },
    "transactions": {
        "transaction_id": [5000],
        "account_id": [1000],
        "transaction_type": ["card_purchase"],
        "amount": [25.5],
        "status": ["completed"],
        "description": ["coffee"],
        "created_at": ["2026-01-01T09:00:00"],
        "updated_at": ["2026-01-01T09:05:00"],
    },
    "transfers": {
        "transfer_id": [8000],
        "source_account_id": [1000],
        "destination_account_id": [1001],
        "amount": [50.0],
        "status": ["completed"],
        "created_at": ["2026-01-01T09:10:00"],
        "updated_at": ["2026-01-01T09:15:00"],
    },
    "payments": {
        "payment_id": [9000],
        "account_id": [1000],
        "merchant_name": ["CloudStream"],
        "category": ["subscription"],
        "amount": [12.5],
        "status": ["completed"],
        "created_at": ["2026-01-01T09:20:00"],
        "updated_at": ["2026-01-01T09:25:00"],
    },
}

SILVER_ROWS = {
    "silver_accounts": {
        "account_id": [1000],
        "updated_at": ["2026-01-01T09:00:00"],
        "bronze_file_path": ["data/raw/cdc/accounts/run.parquet"],
    },
    "silver_transactions": {
        "transaction_id": [5000],
        "updated_at": ["2026-01-01T09:05:00"],
        "bronze_file_path": ["data/raw/cdc/transactions/run.parquet"],
    },
    "silver_transfers": {
        "transfer_id": [8000],
        "updated_at": ["2026-01-01T09:15:00"],
        "bronze_file_path": ["data/raw/cdc/transfers/run.parquet"],
    },
    "silver_payments": {
        "payment_id": [9000],
        "updated_at": ["2026-01-01T09:25:00"],
        "bronze_file_path": ["data/raw/cdc/payments/run.parquet"],
    },
}


def test_validate_bronze_outputs_passes_with_complete_inputs(tmp_path: Path):
    bronze_root = tmp_path / "raw" / "cdc"
    for table_name, payload in BRONZE_ROWS.items():
        table_dir = bronze_root / table_name
        table_dir.mkdir(parents=True)
        pq.write_table(pa.table(payload), table_dir / "run.parquet")

    state_path = tmp_path / "state" / "cdc_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({table: payload["updated_at"][0] for table, payload in BRONZE_ROWS.items()})
    )

    summary = validate_bronze_outputs(bronze_root, state_path)

    assert set(summary["tables"]) == set(BRONZE_ROWS)


def test_validate_bronze_outputs_fails_on_missing_column(tmp_path: Path):
    bronze_root = tmp_path / "raw" / "cdc"
    for table_name, payload in BRONZE_ROWS.items():
        table_dir = bronze_root / table_name
        table_dir.mkdir(parents=True)
        table_payload = dict(payload)
        if table_name == "accounts":
            table_payload.pop("status")
        pq.write_table(pa.table(table_payload), table_dir / "run.parquet")

    state_path = tmp_path / "state" / "cdc_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({table: payload["updated_at"][0] for table, payload in BRONZE_ROWS.items()})
    )

    with pytest.raises(ValidationError, match="missing required columns"):
        validate_bronze_outputs(bronze_root, state_path)


def test_validate_bronze_outputs_fails_on_bad_state(tmp_path: Path):
    bronze_root = tmp_path / "raw" / "cdc"
    for table_name, payload in BRONZE_ROWS.items():
        table_dir = bronze_root / table_name
        table_dir.mkdir(parents=True)
        pq.write_table(pa.table(payload), table_dir / "run.parquet")

    state_path = tmp_path / "state" / "cdc_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({"accounts": "not-a-timestamp"}))

    with pytest.raises(ValidationError, match="missing table entries"):
        validate_bronze_outputs(bronze_root, state_path)


def test_validate_silver_outputs_passes_with_complete_inputs(tmp_path: Path):
    silver_root = tmp_path / "silver"
    silver_root.mkdir(parents=True)
    for model_name, payload in SILVER_ROWS.items():
        pq.write_table(pa.table(payload), silver_root / f"{model_name}.parquet")

    summary = validate_silver_outputs(silver_root)

    assert set(summary["models"]) == set(SILVER_ROWS)


def test_validate_silver_outputs_fails_on_missing_file(tmp_path: Path):
    silver_root = tmp_path / "silver"
    silver_root.mkdir(parents=True)
    for model_name, payload in SILVER_ROWS.items():
        if model_name == "silver_payments":
            continue
        pq.write_table(pa.table(payload), silver_root / f"{model_name}.parquet")

    with pytest.raises(ValidationError, match="Missing silver output"):
        validate_silver_outputs(silver_root)


def test_validate_local_pipeline_can_skip_dbt_tests(tmp_path: Path):
    bronze_root = tmp_path / "raw" / "cdc"
    for table_name, payload in BRONZE_ROWS.items():
        table_dir = bronze_root / table_name
        table_dir.mkdir(parents=True)
        pq.write_table(pa.table(payload), table_dir / "run.parquet")

    silver_root = tmp_path / "silver"
    silver_root.mkdir(parents=True)
    for model_name, payload in SILVER_ROWS.items():
        pq.write_table(pa.table(payload), silver_root / f"{model_name}.parquet")

    state_path = tmp_path / "state" / "cdc_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({table: payload["updated_at"][0] for table, payload in BRONZE_ROWS.items()})
    )

    summary = validate_local_pipeline(
        config=type(
            "Cfg",
            (),
            {
                "bronze_root": bronze_root,
                "state_path": state_path,
                "silver_root": silver_root,
                "profiles_dir": Path("."),
                "project_dir": Path("."),
                "run_dbt_tests": False,
            },
        )()
    )

    assert summary["dbt_test"] is None
