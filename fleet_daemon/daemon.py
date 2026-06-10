"""
fleet-daemon — Real-time MQTT agent for the SuperInstance fleet.

Connects to a pub/sub broker, listens for task signals, executes
workloads, and streams logs back — all while keeping Git as the
permanent source of truth.

Usage:
    python -m fleet_daemon                     # env-based config
    python -m fleet_daemon --config fleet-daemon.yml
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None  # type: ignore

from .config import FleetConfig
from .workflow import GitWorkspace, run_command, format_result

log = logging.getLogger("fleet-daemon")


class FleetDaemon:
    """Persistent background agent that bridges MQTT events to Git tasks."""

    def __init__(self, config: FleetConfig):
        self.config = config
        self.running = False
        self.mqtt_client: Optional[mqtt.Client] = None
        self.workspace: Optional[GitWorkspace] = None
        self._current_task: Optional[str] = None

    # ─── Lifecycle ─────────────────────────────────────

    async def start(self):
        self.running = True
        self._setup_logging()

        log.info("🚀 Fleet Daemon v%s starting", __import__("fleet_daemon").__version__)
        log.info("  Agent ID:  %s", self.config.agent_id)
        log.info("  Broker:    %s", self.config.broker_url)
        log.info("  Work dir:  %s", self.config.repo_dir)

        # Setup git workspace
        if self.config.repo_url:
            self.workspace = GitWorkspace(self.config.repo_url, self.config.repo_dir)
            await self.workspace.setup()
            log.info("  Repo:      %s (cloned)", self.config.repo_url)

        # Connect MQTT
        await self._connect_mqtt()

        # Announce presence
        await self._announce()

        # Main keepalive loop
        try:
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def stop(self):
        self.running = False

    # ─── MQTT ───────────────────────────────────────────

    async def _connect_mqtt(self):
        if mqtt is None:
            log.warning("⚠️ paho-mqtt not installed — running in offline mode")
            return

        client = mqtt.Client(client_id=self.config.agent_id, protocol=mqtt.MQTTv311)
        client.enable_logger(log)

        client.on_connect = self._on_mqtt_connect
        client.on_message = self._on_mqtt_message
        client.on_disconnect = self._on_mqtt_disconnect

        try:
            # Parse broker URL for connect params
            if self.config.broker_url.startswith("ws://") or self.config.broker_url.startswith("wss://"):
                client.connect_async(self.config.broker_url.replace("wss://", "").replace("ws://", "").split(":")[0],
                                     port=int(self.config.broker_url.split(":")[-1].split("/")[0]) if ":" in self.config.broker_url else 8000)
            else:
                host = self.config.broker_url.split(":")[0]
                port = int(self.config.broker_url.split(":")[1].split("/")[0]) if ":" in self.config.broker_url else 1883
                client.connect_async(host, port)
            client.loop_start()
        except Exception as e:
            log.warning("⚠️  MQTT connect failed: %s (continuing in offline mode)", e)

        self.mqtt_client = client

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        log.info("🟢 MQTT connected (rc=%d)", rc)
        # Subscribe to task topic
        client.subscribe(self.config.task_topic)
        log.info("  Subscribed: %s", self.config.task_topic)

    def _on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {"raw": msg.payload.decode("utf-8", errors="replace")}

        log.info("📨 Received on %s: %s", topic, json.dumps(payload, indent=2)[:200])

        # Dispatch task
        task_id = payload.get("task_id") or payload.get("id", "anon-" + str(int(time.time())))
        task_cmd = payload.get("command") or payload.get("cmd", "")
        task_path = payload.get("task_path") or payload.get("path", "")

        if task_cmd or task_path:
            asyncio.run_coroutine_threadsafe(
                self._execute_task(task_id, task_cmd, task_path),
                asyncio.get_event_loop()
            )

    def _on_mqtt_disconnect(self, client, userdata, rc):
        log.warning("🟡 MQTT disconnected (rc=%d)", rc)

    # ─── Announce ───────────────────────────────────────

    async def _announce(self):
        """Announce agent presence on the announce topic."""
        announcement = {
            "id": self.config.agent_id,
            "type": "daemon",
            "status": "online",
            "version": __import__("fleet_daemon").__version__,
            "hostname": os.uname().nodename,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        if self.mqtt_client:
            self.mqtt_client.publish(self.config.announce_topic, json.dumps(announcement))
        log.info("📢 Announced: %s", json.dumps(announcement))

    # ─── Task Execution ─────────────────────────────────

    async def _execute_task(self, task_id: str, command: str, task_path: str):
        """Execute a task and log results."""
        if self._current_task:
            log.warning("⚠️  Already executing task %s, queuing %s", self._current_task, task_id)

        self._current_task = task_id
        start_time = time.monotonic()
        log_lines: list[str] = []

        try:
            # Fetch task config from git if path specified
            if task_path and self.workspace:
                task_config = await self.workspace.fetch_task(task_path)
                if task_config:
                    command = task_config.get("command", command)
                    log.info("📋 Loaded task from %s", task_path)

            if not command:
                log.warning("⚠️  No command specified for task %s", task_id)
                return

            workdir = self.config.repo_dir if self.workspace else None

            # Stream logs
            async for line in run_command(command, cwd=workdir):
                text = line.get("text", "")
                if text:
                    log.info("[%s] %s", task_id[:8], text)
                    log_lines.append(text)
                    self._publish_log(task_id, text)

                if line.get("type") == "result":
                    exit_code = line["exit_code"]
                    duration = time.monotonic() - start_time
                    log.info("✅ Task %s complete (exit=%d, %.1fs)", task_id[:8], exit_code, duration)

                    # Commit result to git
                    if self.workspace:
                        log_dir = self.config.repo_dir / "logs"
                        log_dir.mkdir(exist_ok=True)
                        result = format_result(task_id, exit_code, duration, "\n".join(log_lines[-20:]))
                        result_file = log_dir / f"{task_id}_{int(time.time())}.json"
                        with open(result_file, "w") as f:
                            f.write(result)
                        await self.workspace.commit_result(result_file, f"[daemon] Task {task_id} completed")

        except Exception as e:
            log.error("❌ Task %s failed: %s", task_id[:8], e)
            log_lines.append(f"ERROR: {e}")
        finally:
            self._current_task = None

    def _publish_log(self, task_id: str, text: str):
        """Publish a log line to MQTT."""
        if not self.mqtt_client:
            return
        log_topic = f"{self.config.log_topic}/{self.config.agent_id}"
        payload = json.dumps({
            "ts": time.time(),
            "agent": self.config.agent_id,
            "task": task_id,
            "text": text,
        })
        self.mqtt_client.publish(log_topic, payload)

    # ─── Utilities ──────────────────────────────────────

    def _setup_logging(self):
        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
        )

    async def _shutdown(self):
        log.info("🛑 Shutting down")
        if self.mqtt_client:
            self.mqtt_client.publish(self.config.status_topic, json.dumps({
                "id": self.config.agent_id, "status": "offline"
            }))
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()


# ─── CLI Entrypoint ────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fleet Daemon — MQTT agent for real-time task execution")
    parser.add_argument("--config", "-c", help="Path to YAML config file")
    parser.add_argument("--broker", "-b", help="MQTT broker URL")
    parser.add_argument("--agent-id", "-i", help="Agent identifier")
    parser.add_argument("--repo", "-r", help="Git repo URL for task source")
    parser.add_argument("--workdir", "-w", help="Working directory")
    parser.add_argument("--once", "-1", action="store_true", help="Run one task from config and exit")
    args = parser.parse_args()

    # Load config
    if args.config:
        config = FleetConfig.from_yaml(args.config)
    else:
        config = FleetConfig()

    # CLI overrides
    if args.broker:
        config.broker_url = args.broker
    if args.agent_id:
        config.agent_id = args.agent_id
    if args.repo:
        config.repo_url = args.repo
    if args.workdir:
        config.repo_dir = Path(args.workdir)

    # Run
    daemon = FleetDaemon(config)

    if args.once:
        # Run once mode for testing
        asyncio.run(daemon._execute_task("cli-task", " ".join(sys.argv[sys.argv.index("--")+1:]) if "--" in sys.argv else "echo hello", ""))
        return

    # Handle shutdown signals
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(daemon.stop()))
        except NotImplementedError:
            pass  # Windows
    try:
        loop.run_until_complete(daemon.start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
