from pathlib import Path

from oltp_warehouse.cdc import load_state, save_state, write_parquet_batch


def test_state_round_trip(tmp_path: Path):
    state_path = tmp_path / "cdc_state.json"
    state = {
        "accounts": "2026-01-01T09:00:00+00:00",
        "transactions": None,
    }

    save_state(state_path, state)

    assert load_state(state_path) == state


def test_write_parquet_batch_creates_expected_file(tmp_path: Path):
    rows = [
        {
            "account_id": 1001,
            "customer_id": "CUST-001001",
            "updated_at": "2026-01-01T09:00:00",
        },
        {
            "account_id": 1002,
            "customer_id": "CUST-001002",
            "updated_at": "2026-01-01T09:05:00",
        },
    ]

    output_path = write_parquet_batch(
        table_name="accounts",
        rows=rows,
        output_dir=tmp_path,
        run_id="20260403T120000Z",
    )

    assert output_path == tmp_path / "accounts" / "20260403T120000Z.parquet"
    assert output_path.exists()
