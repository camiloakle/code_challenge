"""Helpers to invoke PySpark pipelines as subprocess tasks."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def project_root() -> Path:
    """Resolve repository root (two levels above airflow/dags/tasks)."""
    return Path(__file__).resolve().parent.parent.parent.parent


def run_module(module: str, extra_args: list[str] | None = None) -> None:
    """Run `python -m module` with PYTHONPATH set to project root."""
    root = project_root()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    cmd = [sys.executable, "-m", module, "--env", "dev"]
    if extra_args:
        cmd.extend(extra_args)
    subprocess.check_call(cmd, cwd=str(root), env=env)
