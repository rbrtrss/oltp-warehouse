from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil

from oltp_warehouse.cdc import ExtractConfig, extract_cdc
from oltp_warehouse.generator import BootstrapConfig, bootstrap_database
from oltp_warehouse.observability import (
    DEFAULT_OBSERVABILITY_DIR,
    RunLogger,
    run_logged_subprocess,
)
from oltp_warehouse.validation import ValidateConfig, validate_local_pipeline


def add_observability_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--observability-dir",
        type=Path,
        default=DEFAULT_OBSERVABILITY_DIR,
        help="Directory for structured run logs and latest summaries.",
    )


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
    add_observability_argument(bootstrap)

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
    add_observability_argument(extract)

    transform = subparsers.add_parser(
        "transform",
        help="Run dbt to build silver warehouse models.",
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
    add_observability_argument(transform)

    validate = subparsers.add_parser(
        "validate",
        help="Validate local bronze and silver outputs and run dbt tests.",
    )
    validate.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("data/raw/cdc"),
        help="Directory containing bronze CDC parquet batches.",
    )
    validate.add_argument(
        "--state-path",
        type=Path,
        default=Path("data/state/cdc_state.json"),
        help="Path to the CDC watermark state file.",
    )
    validate.add_argument(
        "--silver-root",
        type=Path,
        default=Path("data/silver"),
        help="Directory containing silver parquet outputs.",
    )
    validate.add_argument(
        "--profiles-dir",
        type=Path,
        default=Path("."),
        help="Directory containing profiles.yml for dbt.",
    )
    validate.add_argument(
        "--project-dir",
        type=Path,
        default=Path("."),
        help="Repository root containing dbt_project.yml.",
    )
    validate.add_argument(
        "--skip-dbt-tests",
        action="store_true",
        help="Skip dbt test execution and only run file-based validations.",
    )
    add_observability_argument(validate)

    return parser


def run_transform(
    *,
    profiles_dir: Path,
    project_dir: Path,
    select: str,
    logger: RunLogger,
) -> dict[str, object]:
    dbt_executable = shutil.which("dbt")
    if not dbt_executable:
        raise RuntimeError("dbt is not installed. Run `uv sync` or `pip install -e .` first.")

    logger.record_step_started(
        "prepare_transform",
        message="Preparing local warehouse directories for dbt.",
        artifacts={"project_dir": project_dir, "profiles_dir": profiles_dir},
    )
    warehouse_dir = project_dir / "data" / "warehouse"
    silver_dir = project_dir / "data" / "silver"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    logger.record_step_completed(
        "prepare_transform",
        message="Prepared local warehouse directories.",
        artifacts={"warehouse_dir": warehouse_dir, "silver_dir": silver_dir},
    )

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    result = run_logged_subprocess(
        logger=logger,
        step="dbt_run",
        command=[
            dbt_executable,
            "run",
            "--project-dir",
            str(project_dir),
            "--select",
            select,
        ],
        env=env,
        artifact_prefix="dbt-run",
        cwd=project_dir,
    )
    return {
        "selector": select,
        "project_dir": str(project_dir),
        "profiles_dir": str(profiles_dir),
        "warehouse_dir": str(warehouse_dir),
        "silver_dir": str(silver_dir),
        "dbt_run": result,
    }


def emit_observability_for_extract(logger: RunLogger, summary: dict[str, object]) -> None:
    for table_name, table_summary in summary["tables"].items():
        logger.record_metric(
            "extract_table",
            message=f"Extracted CDC rows for {table_name}.",
            metrics={
                "rows": table_summary["rows"],
                "watermark": table_summary["watermark"],
            },
            artifacts={"path": table_summary["path"]},
        )


def emit_observability_for_validate(logger: RunLogger, summary: dict[str, object]) -> None:
    for table_name, table_summary in summary["bronze"]["tables"].items():
        logger.record_metric(
            "validate_bronze",
            message=f"Validated bronze table {table_name}.",
            metrics={
                "files": table_summary["files"],
                "rows": table_summary["rows"],
            },
        )
    for model_name, model_summary in summary["silver"]["models"].items():
        logger.record_metric(
            "validate_silver",
            message=f"Validated silver model {model_name}.",
            metrics={"rows": model_summary["rows"]},
            artifacts={"path": model_summary["path"]},
        )
    if summary["dbt_test"]:
        logger.record_metric(
            "validate_dbt",
            message="dbt tests completed.",
            metrics={
                "duration_ms": summary["dbt_test"]["duration_ms"],
                "returncode": summary["dbt_test"]["returncode"],
            },
            artifacts={
                "stdout_path": summary["dbt_test"]["stdout_path"],
                "stderr_path": summary["dbt_test"]["stderr_path"],
            },
        )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logger = RunLogger(command=args.command, root_dir=args.observability_dir)
    logger.record_run_started(
        message=f"Starting {args.command} command.",
        metadata={
            "command": args.command,
            "observability_dir": args.observability_dir,
        },
    )

    try:
        if args.command == "bootstrap":
            config = BootstrapConfig(
                seed=args.seed,
                accounts=args.accounts,
                transactions=args.transactions,
                transfers=args.transfers,
                payments=args.payments,
            )
            logger.record_step_started(
                "bootstrap_database",
                message="Bootstrapping synthetic OLTP data.",
                metrics={
                    "seed": args.seed,
                    "accounts": args.accounts,
                    "transactions": args.transactions,
                    "transfers": args.transfers,
                    "payments": args.payments,
                },
            )
            summary = bootstrap_database(config)
            logger.record_step_completed(
                "bootstrap_database",
                message="Bootstrapped synthetic OLTP data.",
                metrics=summary,
            )
            print(
                "Bootstrapped synthetic OLTP data: "
                f"{summary['accounts']} accounts, "
                f"{summary['transactions']} transactions, "
                f"{summary['transfers']} transfers, "
                f"{summary['payments']} payments."
            )
            logger.complete_run(message="Bootstrap completed.", summary=summary)
            return 0
        if args.command == "extract-cdc":
            logger.record_step_started(
                "extract_cdc",
                message="Extracting CDC batches from PostgreSQL.",
                artifacts={
                    "output_dir": args.output_dir,
                    "state_path": args.state_path,
                },
            )
            summary = extract_cdc(
                ExtractConfig(
                    output_dir=args.output_dir,
                    state_path=args.state_path,
                )
            )
            emit_observability_for_extract(logger, summary)
            logger.record_step_completed(
                "extract_cdc",
                message="Finished extracting CDC batches.",
                metrics={"tables": len(summary["tables"])},
                artifacts={
                    "output_dir": args.output_dir,
                    "state_path": args.state_path,
                },
            )
            for table_name, table_summary in summary["tables"].items():
                print(
                    f"{table_name}: rows={table_summary['rows']}, "
                    f"path={table_summary['path']}, "
                    f"watermark={table_summary['watermark']}"
                )
            logger.complete_run(message="CDC extraction completed.", summary=summary)
            return 0
        if args.command == "transform":
            summary = run_transform(
                profiles_dir=args.profiles_dir,
                project_dir=args.project_dir,
                select=args.select,
                logger=logger,
            )
            print(f"dbt run completed for selector {args.select}.")
            logger.complete_run(message="Transformation completed.", summary=summary)
            return 0
        if args.command == "validate":
            logger.record_step_started(
                "validate_pipeline",
                message="Validating bronze and silver outputs.",
                artifacts={
                    "bronze_root": args.bronze_root,
                    "silver_root": args.silver_root,
                    "state_path": args.state_path,
                },
            )
            summary = validate_local_pipeline(
                ValidateConfig(
                    bronze_root=args.bronze_root,
                    state_path=args.state_path,
                    silver_root=args.silver_root,
                    profiles_dir=args.profiles_dir,
                    project_dir=args.project_dir,
                    run_dbt_tests=not args.skip_dbt_tests,
                    logger=logger,
                )
            )
            emit_observability_for_validate(logger, summary)
            logger.record_step_completed(
                "validate_pipeline",
                message="Validation checks completed.",
                metrics={
                    "bronze_tables": len(summary["bronze"]["tables"]),
                    "silver_models": len(summary["silver"]["models"]),
                },
            )
            for table_name, table_summary in summary["bronze"]["tables"].items():
                print(
                    f"bronze:{table_name}: files={table_summary['files']}, "
                    f"rows={table_summary['rows']}"
                )
            for model_name, model_summary in summary["silver"]["models"].items():
                print(
                    f"silver:{model_name}: rows={model_summary['rows']}, "
                    f"path={model_summary['path']}"
                )
            if summary["dbt_test"]:
                print("dbt_test: passed")
            logger.complete_run(message="Validation completed.", summary=summary)
            return 0
    except Exception as exc:
        logger.fail_run(exc, message=f"{args.command} failed.")
        raise

    parser.error("Unknown command")
    return 2
