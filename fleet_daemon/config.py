"""Fleet Daemon configuration — YAML config with env overrides."""

from __future__ import annotations
import os
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FleetConfig:
    agent_id: str = field(default_factory=lambda: os.environ.get("FLEET_AGENT_ID", f"agent-{os.uname().nodename}"))
    broker_url: str = field(default_factory=lambda: os.environ.get("FLEET_BROKER_URL", "wss://broker.hivemq.com:8000/mqtt"))
    repo_url: str = field(default_factory=lambda: os.environ.get("FLEET_REPO_URL", ""))
    repo_dir: Path = field(default_factory=lambda: Path(os.environ.get("FLEET_REPO_DIR", "/tmp/fleet-workdir")))
    git_poll_interval: int = int(os.environ.get("FLEET_GIT_POLL", "5"))
    log_level: str = os.environ.get("FLEET_LOG_LEVEL", "INFO")
    task_topic: str = "fleet/tasks"
    log_topic: str = "fleet/logs"
    status_topic: str = "fleet/agent/status"
    announce_topic: str = "fleet/agents"

    @classmethod
    def from_yaml(cls, path: str | Path) -> "FleetConfig":
        import yaml
        p = Path(path)
        if not p.exists():
            return cls()
        with open(p) as f:
            data = yaml.safe_load(f) or {}
        return cls(
            agent_id=data.get("agent_id", cls.agent_id),
            broker_url=data.get("broker_url", cls.broker_url),
            repo_url=data.get("repo_url", cls.repo_url),
            repo_dir=Path(data.get("repo_dir", str(cls.repo_dir))),
            git_poll_interval=data.get("git_poll_interval", cls.git_poll_interval),
            log_level=data.get("log_level", cls.log_level),
            task_topic=data.get("task_topic", cls.task_topic),
            log_topic=data.get("log_topic", cls.log_topic),
            status_topic=data.get("status_topic", cls.status_topic),
            announce_topic=data.get("announce_topic", cls.announce_topic),
        )

    def to_json(self) -> str:
        return json.dumps({
            "agent_id": self.agent_id,
            "broker_url": self.broker_url,
            "repo_url": self.repo_url,
            "repo_dir": str(self.repo_dir),
            "git_poll_interval": self.git_poll_interval,
            "log_level": self.log_level,
            "task_topic": self.task_topic,
            "log_topic": self.log_topic,
            "status_topic": self.status_topic,
            "announce_topic": self.announce_topic,
        }, indent=2)
