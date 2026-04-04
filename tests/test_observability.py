from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from oltp_warehouse import cli
from oltp_warehouse.observability import RunLogger, run_logged_subprocess
from oltp_warehouse import validation
from oltp_warehouse.validation import run_dbt_tests


def read_json_lines(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_run_logger_writes_completed_summary(tmp_path: Path):
    logger = RunLogger(command="extract-cdc", root_dir=tmp_path)

    logger.record_run_started(message="Starting extract.")
    logger.record_metric(
        "extract_table",
        message="Processed accounts.",
        metrics={"rows": 3},
        artifacts={"path": tmp_path / "raw" / "accounts.parquet"},
    )
    summary = logger.complete_run(message="Extract completed.", summary={"rows": 3})

    events = read_json_lines(logger.event_log_path)

    assert events[0]["event_type"] == "run_started"
    assert events[1]["event_type"] == "metric"
    assert events[2]["event_type"] == "run_completed"
    assert summary["status"] == "completed"

    latest = json.loads(logger.latest_summary_path.read_text())
    assert latest["command"] == "extract-cdc"
    assert latest["status"] == "completed"
    assert latest["summary"] == {"rows": 3}


def test_run_logged_subprocess_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    logger = RunLogger(command="transform", root_dir=tmp_path)

    def fake_run(command, check, capture_output, text, env, cwd):
        assert check is False
        assert capture_output is True
        assert text is True
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout="dbt stdout",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    summary = run_logged_subprocess(
        logger=logger,
        step="dbt_run",
        command=["dbt", "run"],
        env={"DBT_PROFILES_DIR": "."},
        artifact_prefix="dbt-run",
        cwd=tmp_path,
    )

    assert summary["returncode"] == 0
    assert Path(summary["stdout_path"]).read_text() == "dbt stdout"
    assert Path(summary["stderr_path"]).read_text() == ""

    events = read_json_lines(logger.event_log_path)
    assert events[0]["event_type"] == "step_started"
    assert events[1]["event_type"] == "step_completed"


def test_run_dbt_tests_records_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    logger = RunLogger(command="validate", root_dir=tmp_path)

    monkeypatch.setattr(validation.shutil, "which", lambda name: "/usr/bin/dbt")

    def fake_run(command, check, capture_output, text, env, cwd):
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout="dbt test output",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    summary = run_dbt_tests(
        profiles_dir=tmp_path,
        project_dir=tmp_path,
        logger=logger,
    )

    assert summary["status"] == "passed"
    assert Path(summary["stdout_path"]).read_text() == "dbt test output"


def test_cli_bootstrap_writes_observability_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setattr(
        cli,
        "bootstrap_database",
        lambda config: {
            "accounts": 2,
            "transactions": 4,
            "transfers": 1,
            "payments": 3,
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oltp-warehouse",
            "bootstrap",
            "--observability-dir",
            str(tmp_path),
        ],
    )

    exit_code = cli.main()

    assert exit_code == 0
    assert "Bootstrapped synthetic OLTP data" in capsys.readouterr().out

    latest = json.loads((tmp_path / "latest" / "bootstrap.json").read_text())
    assert latest["status"] == "completed"
    assert latest["summary"]["accounts"] == 2

    events = read_json_lines(Path(latest["event_log_path"]))
    assert [event["event_type"] for event in events] == [
        "run_started",
        "step_started",
        "step_completed",
        "run_completed",
    ]


def test_cli_extract_failure_writes_failure_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    def fail_extract(config):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(cli, "extract_cdc", fail_extract)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oltp-warehouse",
            "extract-cdc",
            "--observability-dir",
            str(tmp_path),
        ],
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        cli.main()

    latest = json.loads((tmp_path / "latest" / "extract-cdc.json").read_text())
    assert latest["status"] == "failed"
    assert latest["error"]["type"] == "RuntimeError"

    events = read_json_lines(Path(latest["event_log_path"]))
    assert events[-1]["event_type"] == "run_failed"
