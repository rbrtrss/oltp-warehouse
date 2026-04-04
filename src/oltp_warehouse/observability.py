from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
import uuid


DEFAULT_OBSERVABILITY_DIR = Path("data/observability")


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def generate_run_id(command: str) -> str:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{command}-{uuid.uuid4().hex[:8]}"


def to_jsonable(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return format_timestamp(value)
    if isinstance(value, BaseException):
        return {
            "type": type(value).__name__,
            "message": str(value),
        }
    if is_dataclass(value):
        return to_jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


@dataclass(frozen=True)
class RunLogger:
    command: str
    root_dir: Path = DEFAULT_OBSERVABILITY_DIR
    run_id: str | None = None

    def __post_init__(self) -> None:
        actual_run_id = self.run_id or generate_run_id(self.command)
        object.__setattr__(self, "run_id", actual_run_id)
        object.__setattr__(self, "started_at", utc_now())
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.latest_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    @property
    def event_log_path(self) -> Path:
        return self.runs_dir / f"{self.run_id}.jsonl"

    @property
    def runs_dir(self) -> Path:
        return self.root_dir / "runs"

    @property
    def latest_dir(self) -> Path:
        return self.root_dir / "latest"

    @property
    def artifacts_dir(self) -> Path:
        return self.root_dir / "artifacts" / self.run_id

    @property
    def latest_summary_path(self) -> Path:
        return self.latest_dir / f"{self.command}.json"

    def log_event(
        self,
        event_type: str,
        *,
        status: str | None = None,
        step: str | None = None,
        message: str | None = None,
        metrics: dict[str, object] | None = None,
        artifacts: dict[str, object] | None = None,
        error: object | None = None,
        extra: dict[str, object] | None = None,
    ) -> dict[str, object]:
        event = {
            "event_type": event_type,
            "timestamp": format_timestamp(utc_now()),
            "run_id": self.run_id,
            "command": self.command,
        }
        if status is not None:
            event["status"] = status
        if step is not None:
            event["step"] = step
        if message is not None:
            event["message"] = message
        if metrics:
            event["metrics"] = to_jsonable(metrics)
        if artifacts:
            event["artifacts"] = to_jsonable(artifacts)
        if error is not None:
            event["error"] = to_jsonable(error)
        if extra:
            event.update(to_jsonable(extra))

        with self.event_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
        return event

    def record_run_started(
        self,
        *,
        message: str,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.log_event(
            "run_started",
            status="running",
            message=message,
            extra={"metadata": metadata or {}},
        )

    def record_step_started(
        self,
        step: str,
        *,
        message: str,
        metrics: dict[str, object] | None = None,
        artifacts: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.log_event(
            "step_started",
            status="running",
            step=step,
            message=message,
            metrics=metrics,
            artifacts=artifacts,
        )

    def record_step_completed(
        self,
        step: str,
        *,
        message: str,
        metrics: dict[str, object] | None = None,
        artifacts: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.log_event(
            "step_completed",
            status="completed",
            step=step,
            message=message,
            metrics=metrics,
            artifacts=artifacts,
        )

    def record_step_failed(
        self,
        step: str,
        *,
        message: str,
        error: object,
        metrics: dict[str, object] | None = None,
        artifacts: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.log_event(
            "step_failed",
            status="failed",
            step=step,
            message=message,
            metrics=metrics,
            artifacts=artifacts,
            error=error,
        )

    def record_metric(
        self,
        step: str,
        *,
        message: str,
        metrics: dict[str, object],
        artifacts: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.log_event(
            "metric",
            status="running",
            step=step,
            message=message,
            metrics=metrics,
            artifacts=artifacts,
        )

    def complete_run(
        self,
        *,
        message: str,
        summary: dict[str, object] | None = None,
    ) -> dict[str, object]:
        finished_at = utc_now()
        duration_ms = int((finished_at - self.started_at).total_seconds() * 1000)
        payload = {
            "run_id": self.run_id,
            "command": self.command,
            "status": "completed",
            "started_at": format_timestamp(self.started_at),
            "finished_at": format_timestamp(finished_at),
            "duration_ms": duration_ms,
            "event_log_path": str(self.event_log_path),
            "artifacts_dir": str(self.artifacts_dir),
            "summary": to_jsonable(summary or {}),
        }
        self.log_event(
            "run_completed",
            status="completed",
            message=message,
            metrics={"duration_ms": duration_ms},
            artifacts={"event_log_path": self.event_log_path},
            extra={"summary": payload["summary"]},
        )
        self.latest_summary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return payload

    def fail_run(
        self,
        error: BaseException,
        *,
        message: str,
        summary: dict[str, object] | None = None,
    ) -> dict[str, object]:
        finished_at = utc_now()
        duration_ms = int((finished_at - self.started_at).total_seconds() * 1000)
        payload = {
            "run_id": self.run_id,
            "command": self.command,
            "status": "failed",
            "started_at": format_timestamp(self.started_at),
            "finished_at": format_timestamp(finished_at),
            "duration_ms": duration_ms,
            "event_log_path": str(self.event_log_path),
            "artifacts_dir": str(self.artifacts_dir),
            "summary": to_jsonable(summary or {}),
            "error": to_jsonable(error),
        }
        self.log_event(
            "run_failed",
            status="failed",
            message=message,
            metrics={"duration_ms": duration_ms},
            artifacts={"event_log_path": self.event_log_path},
            error=error,
            extra={"summary": payload["summary"]},
        )
        self.latest_summary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return payload

    def write_artifact(self, name: str, content: str) -> Path:
        artifact_path = self.artifacts_dir / name
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(content, encoding="utf-8")
        return artifact_path


def run_logged_subprocess(
    *,
    logger: RunLogger,
    step: str,
    command: list[str],
    env: dict[str, str] | None = None,
    artifact_prefix: str,
    cwd: Path | None = None,
) -> dict[str, object]:
    logger.record_step_started(
        step,
        message=f"Running {' '.join(command)}",
        artifacts={"cwd": cwd} if cwd else None,
    )
    started_at = utc_now()
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(cwd) if cwd else None,
    )
    duration_ms = int((utc_now() - started_at).total_seconds() * 1000)
    stdout_path = logger.write_artifact(f"{artifact_prefix}.stdout.log", completed.stdout)
    stderr_path = logger.write_artifact(f"{artifact_prefix}.stderr.log", completed.stderr)
    result = {
        "command": command,
        "returncode": completed.returncode,
        "duration_ms": duration_ms,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }
    artifacts = {
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
    }
    if completed.returncode != 0:
        error = subprocess.CalledProcessError(
            completed.returncode,
            command,
            output=completed.stdout,
            stderr=completed.stderr,
        )
        logger.record_step_failed(
            step,
            message=f"Subprocess failed: {' '.join(command)}",
            error=error,
            metrics={"duration_ms": duration_ms, "returncode": completed.returncode},
            artifacts=artifacts,
        )
        raise error

    logger.record_step_completed(
        step,
        message=f"Completed {' '.join(command)}",
        metrics={"duration_ms": duration_ms, "returncode": completed.returncode},
        artifacts=artifacts,
    )
    return result
