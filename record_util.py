#!/usr/bin/env python3
"""Record laptop + InnoPool slave utilization for offline review.

Example (a few hours):
  cd /home/kevin/tig-monorepo/tig-benchmarker
  python3 /home/kevin/innopool-slave/record_util.py --hours 3

Outputs JSONL under ./util_logs/ by default. Zip/send that file afterward.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


TELEMETRY_RE = re.compile(
    r"fetching batches from .* "
    r"state=(?P<state>\S+) "
    r"active=(?P<active>\d+) "
    r"pending=(?P<pending>\d+) "
    r"last_idle_ms=(?P<last_idle_ms>\d+) "
    r"version=(?P<version>\S+)"
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_loadavg():
    try:
        one, five, fifteen = os.getloadavg()
        return {"load_1m": one, "load_5m": five, "load_15m": fifteen}
    except OSError:
        return {}


def read_meminfo():
    out = {}
    try:
        info = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[0].endswith(":"):
                    info[parts[0][:-1]] = int(parts[1])  # kB
        total = info.get("MemTotal")
        avail = info.get("MemAvailable") or info.get("MemFree")
        if total:
            out["ram_total_gb"] = round(total / (1024 * 1024), 2)
            if avail is not None:
                out["ram_avail_gb"] = round(avail / (1024 * 1024), 2)
                out["ram_used_pct"] = round(100.0 * (total - avail) / total, 1)
    except OSError:
        pass
    return out


def read_cpu_times():
    try:
        with open("/proc/stat", "r", encoding="utf-8") as f:
            parts = f.readline().split()
        # user nice system idle iowait irq softirq steal ...
        vals = [int(x) for x in parts[1:8]]
        idle = vals[3] + vals[4]
        total = sum(vals)
        return idle, total
    except OSError:
        return None


def cpu_percent(prev, sample_s: float = 0.25) -> float | None:
    if prev is None:
        a = read_cpu_times()
        time.sleep(sample_s)
        b = read_cpu_times()
    else:
        a = prev
        time.sleep(sample_s)
        b = read_cpu_times()
    if not a or not b:
        return None
    idle_d = b[0] - a[0]
    total_d = b[1] - a[1]
    if total_d <= 0:
        return None
    return round(100.0 * (1.0 - (idle_d / total_d)), 1)


def _running_containers() -> set[str]:
    try:
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return set()
    if proc.returncode != 0:
        return set()
    return {line.strip() for line in proc.stdout.splitlines() if line.strip()}


def docker_stats(container_names: list[str]) -> dict:
    wanted = [n for n in container_names if n]
    running = _running_containers()
    # Prefer exact matches; also accept compose-style *-slave-1 if "slave" requested.
    names = []
    for name in wanted:
        if name in running:
            names.append(name)
        elif name == "slave":
            names.extend(sorted(n for n in running if n.endswith("-slave-1") or n == "slave"))
    names = list(dict.fromkeys(names))
    if not names:
        return {"containers": {}, "docker_stats_note": "no matching running containers"}
    try:
        proc = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}",
                *names,
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    if proc.returncode != 0:
        return {"docker_stats_error": (proc.stderr or proc.stdout).strip()[:300]}
    out = {"containers": {}}
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        name, cpu, mem, mem_pct = parts[0], parts[1], parts[2], parts[3]
        out["containers"][name] = {
            "cpu_perc": cpu.replace("%", ""),
            "mem_usage": mem,
            "mem_perc": mem_pct.replace("%", ""),
        }
    return out


def latest_slave_telemetry(compose_file: Path, service: str) -> dict:
    if not compose_file.exists():
        return {}
    try:
        proc = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "logs",
                "--no-color",
                "--tail",
                "80",
                service,
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
            cwd=str(compose_file.parent),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    text = proc.stdout or ""
    matches = list(TELEMETRY_RE.finditer(text))
    if not matches:
        # fallback: any innopool-slave version line
        if "innopool-slave/" in text:
            return {"slave_log_seen": True}
        return {}
    m = matches[-1].groupdict()
    return {
        "slave_state": m["state"],
        "slave_active_batches": int(m["active"]),
        "slave_pending_batches": int(m["pending"]),
        "slave_last_idle_ms": int(m["last_idle_ms"]),
        "slave_version": m["version"],
    }


def nproc() -> int:
    return os.cpu_count() or 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hours", type=float, default=3.0, help="How long to record (default 3)")
    ap.add_argument("--interval", type=float, default=10.0, help="Seconds between samples (default 10)")
    ap.add_argument(
        "--compose-file",
        default="slave.yml",
        help="docker compose file relative to cwd (default slave.yml)",
    )
    ap.add_argument("--service", default="slave", help="compose service name (default slave)")
    ap.add_argument(
        "--containers",
        default="slave",
        help="Comma-separated container names for docker stats (default: slave)",
    )
    ap.add_argument(
        "--out-dir",
        default="util_logs",
        help="Output directory (default ./util_logs)",
    )
    args = ap.parse_args()

    compose_file = Path(args.compose_file).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"util_{stamp}.jsonl"

    duration_s = max(1.0, args.hours * 3600.0)
    interval_s = max(2.0, float(args.interval))
    containers = [c.strip() for c in args.containers.split(",") if c.strip()]

    meta = {
        "type": "meta",
        "started_utc": utc_now_iso(),
        "hours": args.hours,
        "interval_s": interval_s,
        "cores": nproc(),
        "cwd": os.getcwd(),
        "compose_file": str(compose_file),
        "out_path": str(out_path),
        "hostname": os.uname().nodename if hasattr(os, "uname") else "",
    }
    print(f"Recording to {out_path}", flush=True)
    print(json.dumps(meta), flush=True)

    end_at = time.time() + duration_s
    samples = 0
    with out_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(meta) + "\n")
        f.flush()
        prev_cpu = read_cpu_times()
        while time.time() < end_at:
            loop_start = time.time()
            cpu = cpu_percent(prev_cpu, sample_s=0.2)
            prev_cpu = read_cpu_times()
            row = {
                "type": "sample",
                "ts_utc": utc_now_iso(),
                "epoch_ms": int(time.time() * 1000),
                "cpu_pct": cpu,
                "cores": nproc(),
            }
            row.update(read_loadavg())
            row.update(read_meminfo())
            row.update(docker_stats(containers))
            row.update(latest_slave_telemetry(compose_file, args.service))
            if row.get("load_1m") is not None and row.get("cores"):
                row["load_per_core"] = round(float(row["load_1m"]) / float(row["cores"]), 3)
            f.write(json.dumps(row) + "\n")
            f.flush()
            samples += 1
            if samples % 30 == 0:
                print(
                    f"[{row['ts_utc']}] samples={samples} cpu={row.get('cpu_pct')}% "
                    f"load1={row.get('load_1m')} state={row.get('slave_state')} "
                    f"idle_ms={row.get('slave_last_idle_ms')}",
                    flush=True,
                )
            # account for sampling time
            slept = time.time() - loop_start
            time.sleep(max(0.1, interval_s - slept))

        footer = {
            "type": "footer",
            "ended_utc": utc_now_iso(),
            "samples": samples,
            "out_path": str(out_path),
        }
        f.write(json.dumps(footer) + "\n")
        f.flush()

    print(f"Done. Wrote {samples} samples to {out_path}", flush=True)
    print("Send that .jsonl file back for analysis.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
