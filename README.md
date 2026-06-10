# 🧰 Fleet Daemon

Real-time MQTT agent daemon for the SuperInstance multi-agent C2 matrix.
Replaces 30s git-polling loops with sub-second event-driven task dispatch.

## Quick Start

```bash
pip install paho-mqtt pyyaml

# Configure
export FLEET_AGENT_ID="oracle2"
export FLEET_BROKER_URL="wss://broker.hivemq.com:8000/mqtt"
export FLEET_REPO_URL="https://github.com/SuperInstance/onboard"

# Run
fleet-daemon
```

## Architecture

```
MQTT Broker (HiveMQ/Ably)
  ↕  persistent outbound TCP
Fleet Daemon (this)
  ↕  git pull / commit
Git Repository (source of truth)
```

## MQTT Topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `fleet/tasks` | ← Inbound | Task dispatch (JSON with command, task_id, task_path) |
| `fleet/logs/<agent>` | → Outbound | Real-time stdout/stderr streaming |
| `fleet/agent/status` | → Outbound | Online/offline heartbeats |
| `fleet/agents` | → Outbound | Agent registration announcements |

## Configuration

Via YAML:
```bash
fleet-daemon --config fleet-daemon.yml
```

Via env vars:
```bash
export FLEET_AGENT_ID=oracle2
export FLEET_BROKER_URL=wss://broker.hivemq.com:8000/mqtt
export FLEET_REPO_URL=https://github.com/SuperInstance/onboard
export FLEET_REPO_DIR=/tmp/fleet-workdir
fleet-daemon
```

Via CLI flags:
```bash
fleet-daemon --agent-id oracle2 --broker wss://broker.hivemq.com:8000/mqtt --repo https://github.com/SuperInstance/onboard
```

## Task Payload Format

Send to `fleet/tasks`:
```json
{
  "task_id": "build-pincher-001",
  "command": "cd /workspace && cargo build --release",
  "task_path": "tasks/build_pincher.json"
}
```

## systemd Service

```
[Unit]
Description=Fleet Daemon
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/fleet-daemon --config /etc/fleet/daemon.yml
Restart=always
User=ubuntu
Environment=FLEET_AGENT_ID=oracle2

[Install]
WantedBy=multi-user.target
```
