"""Task execution engine — pulls payloads, runs commands, reports results."""

from __future__ import annotations
import asyncio
import os
import json
import time
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import AsyncIterator


async def run_command(cmd: str, cwd: Path | None = None) -> AsyncIterator[dict]:
    """Run a shell command, yielding stdout/stderr lines in real-time."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=cwd or Path.cwd(),
    )
    assert proc.stdout is not None
    start = time.monotonic()
    async for line in proc.stdout:
        text = line.decode("utf-8", errors="replace").rstrip()
        yield {
            "type": "log",
            "ts": time.time(),
            "elapsed": round(time.monotonic() - start, 3),
            "text": text,
        }
    exit_code = await proc.wait()
    yield {
        "type": "result",
        "ts": time.time(),
        "elapsed": round(time.monotonic() - start, 3),
        "exit_code": exit_code,
        "success": exit_code == 0,
    }


class GitWorkspace:
    """Manages a local git clone for task execution."""

    def __init__(self, repo_url: str, workdir: Path):
        self.repo_url = repo_url
        self.workdir = workdir

    async def setup(self) -> Path:
        """Clone or pull the repo."""
        if not self.workdir.exists():
            self.workdir.mkdir(parents=True, exist_ok=True)
            proc = await asyncio.create_subprocess_exec(
                "git", "clone", self.repo_url, str(self.workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            await proc.wait()
        else:
            proc = await asyncio.create_subprocess_exec(
                "git", "-C", str(self.workdir), "pull",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            await proc.wait()
        return self.workdir

    async def fetch_task(self, task_path: str) -> dict | None:
        """Load a task JSON from the repo."""
        fpath = self.workdir / task_path
        if not fpath.exists():
            return None
        with open(fpath) as f:
            return json.load(f)

    async def commit_result(self, result_path: Path, message: str = "fleet-daemon: task complete") -> bool:
        """Commit a result file and push."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "-C", str(self.workdir), "add", str(result_path.relative_to(self.workdir)),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
            proc = await asyncio.create_subprocess_exec(
                "git", "-C", str(self.workdir), "commit", "-m", message,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
            proc = await asyncio.create_subprocess_exec(
                "git", "-C", str(self.workdir), "push",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
            return True
        except Exception:
            return False


def format_result(task_id: str, exit_code: int, duration: float, log_preview: str) -> str:
    """Format a task result for commit to /logs/."""
    return json.dumps({
        "task_id": task_id,
        "status": "ok" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "duration_seconds": round(duration, 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "agent_id": os.environ.get("FLEET_AGENT_ID", "unknown"),
        "log_preview": log_preview[:1000],
    }, indent=2)
