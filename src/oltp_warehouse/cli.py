from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import os

from oltp_warehouse.cdc import ExtractConfig, extract_cdc
from oltp_warehouse.generator import BootstrapConfig, bootstrap_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="oltp-warehouse",
        description="Bootstrap a synthetic PostgreSQL OLTP dataset.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Drop and recreate the demo schema, then seed synthetic data.",
    )
    bootstrap.add_argument("--seed", type=int, default=42, help="Random seed.")
    bootstrap.add_argument(
        "--accounts",
        type=int,
        default=25,
        help="Number of accounts to create.",
    )
    bootstrap.add_argument(
        "--transactions",
        type=int,
        default=120,
        help="Number of transactions to create.",
    )
    bootstrap.add_argument(
        "--transfers",
        type=int,
        default=40,
        help="Number of transfers to create.",
    )
    bootstrap.add_argument(
        "--payments",
        type=int,
        default=60,
        help="Number of payments to create.",
    )

    extract = subparsers.add_parser(
        "extract-cdc",
        help="Extract changed rows from PostgreSQL into parquet batches.",
    )
    extract.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/cdc"),
        help="Directory for parquet batch outputs.",
    )
    extract.add_argument(
        "--state-path",
        type=Path,
        default=Path("data/state/cdc_state.json"),
        help="Path to the watermark state file.",
    )

    transform = subparsers.add_parser(
        "transform",
        help="Show the dbt command used to build silver warehouse models.",
    )
    transform.add_argument(
        "--profiles-dir",
        type=Path,
        default=Path("."),
        help="Directory containing profiles.yml for dbt.",
    )
    transform.add_argument(
        "--project-dir",
        type=Path,
        default=Path("."),
        help="Repository root containing dbt_project.yml.",
    )
    transform.add_argument(
        "--select",
        default="silver",
        help="dbt model selector to run.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "bootstrap":
        config = BootstrapConfig(
            seed=args.seed,
            accounts=args.accounts,
            transactions=args.transactions,
            transfers=args.transfers,
            payments=args.payments,
        )
        summary = bootstrap_database(config)
        print(
            "Bootstrapped synthetic OLTP data: "
            f"{summary['accounts']} accounts, "
            f"{summary['transactions']} transactions, "
            f"{summary['transfers']} transfers, "
            f"{summary['payments']} payments."
        )
        return 0
    if args.command == "extract-cdc":
        summary = extract_cdc(
            ExtractConfig(
                output_dir=args.output_dir,
                state_path=args.state_path,
            )
        )
        for table_name, table_summary in summary["tables"].items():
            print(
                f"{table_name}: rows={table_summary['rows']}, "
                f"path={table_summary['path']}, "
                f"watermark={table_summary['watermark']}"
            )
        return 0
    if args.command == "transform":
        dbt_executable = shutil.which("dbt")
        if not dbt_executable:
            parser.error(
                "dbt is not installed. Run `uv sync` or `pip install -e .` first."
            )
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(args.profiles_dir)
        subprocess.run(
            [
                dbt_executable,
                "run",
                "--project-dir",
                str(args.project_dir),
                "--select",
                args.select,
            ],
            check=True,
            env=env,
        )
        return 0

    parser.error("Unknown command")
    return 2
