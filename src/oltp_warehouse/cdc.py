from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from oltp_warehouse.config import DatabaseConfig

if TYPE_CHECKING:
    from psycopg import Cursor

TABLE_SPECS = (
    ("accounts", "account_id"),
    ("transactions", "transaction_id"),
    ("transfers", "transfer_id"),
    ("payments", "payment_id"),
)

DEFAULT_OUTPUT_DIR = Path("data/raw/cdc")
DEFAULT_STATE_PATH = Path("data/state/cdc_state.json")


@dataclass(frozen=True)
class ExtractConfig:
    output_dir: Path = DEFAULT_OUTPUT_DIR
    state_path: Path = DEFAULT_STATE_PATH


def extract_cdc(config: ExtractConfig) -> dict[str, object]:
    from psycopg import connect
    from psycopg.rows import dict_row

    db_config = DatabaseConfig.from_env()
    state = load_state(config.state_path)
    pending_watermarks: dict[str, str] = {}
    table_summaries: dict[str, dict[str, object]] = {}
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    with connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.dbname,
        user=db_config.user,
        password=db_config.password,
        row_factory=dict_row,
    ) as connection:
        with connection.cursor() as cursor:
            for table_name, primary_key in TABLE_SPECS:
                rows = fetch_changed_rows(
                    cursor,
                    table_name=table_name,
                    primary_key=primary_key,
                    watermark=state.get(table_name),
                )
                written_path = None
                next_watermark = state.get(table_name)
                if rows:
                    written_path = write_parquet_batch(
                        table_name=table_name,
                        rows=rows,
                        output_dir=config.output_dir,
                        run_id=run_id,
                    )
                    next_watermark = rows[-1]["updated_at"].isoformat()
                pending_watermarks[table_name] = next_watermark
                table_summaries[table_name] = {
                    "rows": len(rows),
                    "path": str(written_path) if written_path else None,
                    "watermark": next_watermark,
                }

    save_state(config.state_path, pending_watermarks)
    return {
        "run_id": run_id,
        "output_dir": str(config.output_dir),
        "state_path": str(config.state_path),
        "tables": table_summaries,
    }


def fetch_changed_rows(
    cursor: Cursor,
    *,
    table_name: str,
    primary_key: str,
    watermark: str | None,
) -> list[dict[str, object]]:
    if watermark:
        cursor.execute(
            f"""
            SELECT *
            FROM {table_name}
            WHERE updated_at > %s
            ORDER BY updated_at, {primary_key}
            """,
            (watermark,),
        )
    else:
        cursor.execute(
            f"""
            SELECT *
            FROM {table_name}
            ORDER BY updated_at, {primary_key}
            """
        )
    return list(cursor.fetchall())


def write_parquet_batch(
    *,
    table_name: str,
    rows: list[dict[str, object]],
    output_dir: Path,
    run_id: str,
) -> Path:
    table_dir = output_dir / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
    output_path = table_dir / f"{run_id}.parquet"

    columns = {key: [row[key] for row in rows] for key in rows[0]}
    pq.write_table(pa.table(columns), output_path)
    return output_path


def load_state(state_path: Path) -> dict[str, str | None]:
    if not state_path.exists():
        return {}

    payload = json.loads(state_path.read_text())
    return {str(key): value for key, value in payload.items()}


def save_state(state_path: Path, state: dict[str, str | None]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    temp_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    temp_path.replace(state_path)
