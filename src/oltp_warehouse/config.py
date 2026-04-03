from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(dotenv_path: str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "oltp_warehouse"
    user: str = "oltp"
    password: str = "oltp"

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        load_dotenv()
        return cls(
            host=os.getenv("OLTP_DB_HOST", "localhost"),
            port=int(os.getenv("OLTP_DB_PORT", "5432")),
            dbname=os.getenv("OLTP_DB_NAME", "oltp_warehouse"),
            user=os.getenv("OLTP_DB_USER", "oltp"),
            password=os.getenv("OLTP_DB_PASSWORD", "oltp"),
        )
