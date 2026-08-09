#!/usr/bin/env python3
"""InnoPool custom slave — stock TIG slave + telemetry + fast poll.

Drop-in replacement for tig-benchmarker/slave/main.py.
"""

import io
import json
import mimetypes
import os
import platform
import logging
import randomname
import requests
import shutil
import subprocess
import sys
import tarfile
import time
import zlib
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue
from threading import Event, Lock, Thread
from urllib.parse import urlparse

from common.structs import OutputData, MerkleProof
from common.merkle_tree import MerkleTree, MerkleHash

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

SLAVE_VERSION = os.getenv("INNOPOOL_SLAVE_VERSION") or "innopool-slave/0.1.4"
IDLE_POLL_SEC = float(os.getenv("INNOPOOL_IDLE_POLL_SEC", "1.0"))
BUSY_POLL_SEC = float(os.getenv("INNOPOOL_BUSY_POLL_SEC", "3.0"))
ERROR_POLL_SEC = float(os.getenv("INNOPOOL_ERROR_POLL_SEC", "5.0"))

PENDING_BATCH_IDS = set()
PROCESSING_BATCH_IDS = {}
READY_BATCH_IDS = set()
# Root and proof phases share the same batch id (benchmark_id_batch_idx).
# Track them separately or proof handoffs get treated as already-submitted roots.
FINISHED_ROOT_BATCH_IDS = {}
FINISHED_PROOF_BATCH_IDS = {}

_STATE_LOCK = Lock()
_DOWNLOADING = 0
_LAST_IDLE_GAP_MS = 0
_IDLE_SINCE_MS = None
_POLL_WAKE = Event()

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8787"))
DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR") or Path(__file__).resolve().parent / "dashboard")
RECENT_LIMIT = 40

# Dashboard identity (set in main)
_SLAVE_NAME = ""
_MASTER_LABEL = ""
_NUM_WORKERS = 1
_STATUS_LOCK = Lock()
_CURRENT = None  # active batch card
_RECENT = deque(maxlen=RECENT_LIMIT)

if (CPU_ARCH := platform.machine().lower()) in ["x86_64", "amd64"]:
    CPU_ARCH = "amd64"
elif CPU_ARCH in ["arm64", "aarch64"]:
    CPU_ARCH = "arm64"
else:
    print(f"Unsupported CPU architecture: {CPU_ARCH}")
    sys.exit(1)


def now():
    return int(time.time() * 1000)


def _batch_meta(batch: dict) -> dict:
    settings = batch.get("settings") or {}
    is_proof = batch.get("sampled_nonces") is not None
    return {
        "batch_id": batch.get("id"),
        "challenge": batch.get("challenge"),
        "challenge_id": settings.get("challenge_id"),
        "algorithm": batch.get("algorithm"),
        "algorithm_id": settings.get("algorithm_id"),
        "track_id": settings.get("track_id"),
        "phase": "proof" if is_proof else "root",
        "nonces_total": int(batch.get("num_nonces") or 0),
    }


def _status_set_current(batch: dict, nonces_done: int = 0, started_ms=None):
    global _CURRENT
    meta = _batch_meta(batch)
    with _STATUS_LOCK:
        prev = _CURRENT or {}
        start = started_ms or prev.get("started_ms") or now()
        _CURRENT = {
            **meta,
            "nonces_done": int(nonces_done),
            "started_ms": start,
        }


def _status_update_progress(batch_id: str, nonces_done: int, nonces_total=None):
    with _STATUS_LOCK:
        if not _CURRENT or _CURRENT.get("batch_id") != batch_id:
            return
        _CURRENT["nonces_done"] = int(nonces_done)
        if nonces_total is not None:
            _CURRENT["nonces_total"] = int(nonces_total)


def _status_clear_current(batch_id=None):
    global _CURRENT
    with _STATUS_LOCK:
        if batch_id is None or (_CURRENT and _CURRENT.get("batch_id") == batch_id):
            _CURRENT = None


def _status_push_recent(batch: dict, status: str, error=None):
    global _CURRENT
    meta = _batch_meta(batch)
    item = {
        **meta,
        "status": status,
        "error": (error or "")[:180] or None,
        "ts_ms": now(),
    }
    with _STATUS_LOCK:
        _RECENT.appendleft(item)
        if _CURRENT and _CURRENT.get("batch_id") == meta.get("batch_id"):
            _CURRENT = None


def _display_state(current) -> str:
    """User-facing state for the dashboard (not the master telemetry state).

    Prefer what the operator sees in Current work: active compute beats a
    leftover READY submit queue from a previous batch.
    """
    with _STATE_LOCK:
        downloading = _DOWNLOADING > 0
        processing = bool(PROCESSING_BATCH_IDS)
        pending = bool(PENDING_BATCH_IDS)
        ready = bool(READY_BATCH_IDS)
    if downloading:
        return "downloading"
    if processing:
        return "running"
    if current:
        done = int(current.get("nonces_done") or 0)
        total = int(current.get("nonces_total") or 0)
        if total > 0 and done < total:
            return "running"
        # Current batch finished compute — waiting on submit path.
        if ready:
            return "submitting"
    if ready:
        return "submitting"
    if pending:
        return "running"
    return "idle"


def _status_snapshot(num_workers: int) -> dict:
    telem = _collect_host_telemetry(num_workers)
    cores = int(telem.get("cores") or 1)
    load = telem.get("load_1m")
    with _STATUS_LOCK:
        current = dict(_CURRENT) if _CURRENT else None
        recent = list(_RECENT)
    if current and current.get("started_ms"):
        current["elapsed_ms"] = max(0, now() - int(current["started_ms"]))
    display = _display_state(current)
    return {
        "slave_name": _SLAVE_NAME,
        "version": SLAVE_VERSION,
        "master": _MASTER_LABEL,
        "state": display,
        "runtime_state": telem.get("state"),
        "cores": cores,
        "num_workers": int(telem.get("num_workers") or num_workers),
        "load_1m": load,
        "load_per_core": (float(load) / cores) if load is not None else None,
        "ram_gb": telem.get("ram_gb"),
        "free_ram_gb": telem.get("free_ram_gb"),
        "last_idle_ms": telem.get("last_idle_ms"),
        "active_batches": telem.get("active_batches"),
        "pending_batches": telem.get("pending_batches"),
        "current": current,
        "recent": recent,
        "ts_ms": now(),
    }


def _start_dashboard_server(num_workers: int):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            logger.debug("dashboard: " + fmt, *args)

        def _send(self, code: int, body: bytes, content_type: str):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/api/status":
                payload = json.dumps(_status_snapshot(num_workers)).encode()
                self._send(200, payload, "application/json; charset=utf-8")
                return
            rel = "index.html" if path in ("/", "") else path.lstrip("/")
            if ".." in rel or rel.startswith("/"):
                self._send(404, b"not found", "text/plain; charset=utf-8")
                return
            file_path = DASHBOARD_DIR / rel
            if not file_path.is_file():
                self._send(404, b"not found", "text/plain; charset=utf-8")
                return
            data = file_path.read_bytes()
            ctype = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            self._send(200, data, ctype)

    try:
        server = ThreadingHTTPServer((DASHBOARD_HOST, DASHBOARD_PORT), Handler)
    except OSError as exc:
        logger.error("dashboard server failed to bind %s:%s: %s", DASHBOARD_HOST, DASHBOARD_PORT, exc)
        return
    thread = Thread(target=server.serve_forever, name="dashboard", daemon=True)
    thread.start()
    logger.info(
        "dashboard listening on http://%s:%s (dir=%s)",
        DASHBOARD_HOST,
        DASHBOARD_PORT,
        DASHBOARD_DIR,
    )


def _mark_busy():
    global _IDLE_SINCE_MS, _LAST_IDLE_GAP_MS
    with _STATE_LOCK:
        if _IDLE_SINCE_MS is not None:
            _LAST_IDLE_GAP_MS = max(0, now() - _IDLE_SINCE_MS)
            _IDLE_SINCE_MS = None


def _mark_idle_if_quiet():
    global _IDLE_SINCE_MS
    with _STATE_LOCK:
        busy = bool(PENDING_BATCH_IDS or PROCESSING_BATCH_IDS or READY_BATCH_IDS or _DOWNLOADING)
        if busy:
            return
        if _IDLE_SINCE_MS is None:
            _IDLE_SINCE_MS = now()


def _runtime_state() -> str:
    with _STATE_LOCK:
        if _DOWNLOADING > 0:
            return "downloading"
        if READY_BATCH_IDS:
            return "submitting"
        if PROCESSING_BATCH_IDS or PENDING_BATCH_IDS:
            return "running"
        return "idle"


def _last_idle_ms() -> int:
    with _STATE_LOCK:
        if _IDLE_SINCE_MS is not None:
            return max(0, now() - _IDLE_SINCE_MS)
        return int(_LAST_IDLE_GAP_MS or 0)


def _wake_poll():
    _POLL_WAKE.set()


def _collect_host_telemetry(num_workers: int) -> dict:
    cores = os.cpu_count() or 1
    telem = {
        "cores": cores,
        "num_workers": max(1, int(num_workers)),
        "slave_version": SLAVE_VERSION,
        "state": _runtime_state(),
        "active_batches": len(PROCESSING_BATCH_IDS),
        "pending_batches": len(PENDING_BATCH_IDS),
        "last_idle_ms": _last_idle_ms(),
    }
    try:
        telem["load_1m"] = float(os.getloadavg()[0])
    except (AttributeError, OSError):
        pass
    try:
        # Prefer /proc so we do not require psutil in the image.
        page_size = os.sysconf("SC_PAGE_SIZE")
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            info = {}
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[0].endswith(":"):
                    info[parts[0][:-1]] = int(parts[1])
        mem_total_kb = info.get("MemTotal")
        mem_avail_kb = info.get("MemAvailable") or info.get("MemFree")
        if mem_total_kb:
            telem["ram_gb"] = max(1, mem_total_kb // (1024 * 1024))
        if mem_avail_kb is not None:
            telem["free_ram_gb"] = round(mem_avail_kb / (1024 * 1024), 1)
        _ = page_size  # quiet linters; kept for future RSS sampling
    except Exception:
        pass
    return telem


def _telemetry_headers(telem: dict) -> dict:
    mapping = {
        "cores": "X-InnoPool-Cores",
        "num_workers": "X-InnoPool-Num-Workers",
        "load_1m": "X-InnoPool-Load-1m",
        "ram_gb": "X-InnoPool-Ram-Gb",
        "free_ram_gb": "X-InnoPool-Free-Ram-Gb",
        "gpu_util": "X-InnoPool-Gpu-Util",
        "gpu_vram_free_mb": "X-InnoPool-Gpu-Vram-Free-Mb",
        "state": "X-InnoPool-State",
        "active_batches": "X-InnoPool-Active-Batches",
        "pending_batches": "X-InnoPool-Pending-Batches",
        "last_idle_ms": "X-InnoPool-Last-Idle-Ms",
        "slave_version": "X-InnoPool-Slave-Version",
    }
    headers = {}
    for key, header in mapping.items():
        if key in telem and telem[key] is not None:
            headers[header] = str(telem[key])
    return headers


def download_library(algorithms_dir, batch):
    global _DOWNLOADING
    challenge_folder = f"{algorithms_dir}/{batch['challenge']}"
    so_path = f"{challenge_folder}/{CPU_ARCH}/{batch['algorithm']}.so"
    ptx_path = f"{challenge_folder}/ptx/{batch['algorithm']}.ptx"
    if not os.path.exists(so_path):
        start = now()
        logger.info(f"downloading {batch['algorithm']}.tar.gz from {batch['download_url']}")
        with _STATE_LOCK:
            _DOWNLOADING += 1
        try:
            resp = requests.get(batch["download_url"], stream=True)
            if resp.status_code != 200:
                raise Exception(f"status {resp.status_code} when downloading algorithm library: {resp.text}")
            with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
                # Python 3.12+ / 3.14: pass filter to avoid DeprecationWarning
                # and upcoming default data-filter behavior.
                try:
                    tar.extractall(path=challenge_folder, filter="data")
                except TypeError:
                    tar.extractall(path=challenge_folder)
            logger.debug(f"downloading {batch['algorithm']}.tar.gz took {now() - start}ms")
        finally:
            with _STATE_LOCK:
                _DOWNLOADING = max(0, _DOWNLOADING - 1)

    if not os.path.exists(ptx_path):
        return so_path, None
    return so_path, ptx_path


def run_tig_runtime(nonce, batch, so_path, ptx_path, results_dir):
    output_dir = f"{results_dir}/{batch['id']}"
    output_file = f"{output_dir}/{nonce}.json"
    settings = json.dumps(batch["settings"], separators=(",", ":"))
    start = now()
    cmd = [
        "docker", "exec", batch["challenge"], "tig-runtime",
        settings,
        batch["rand_hash"],
        str(nonce),
        so_path,
        "--fuel", str(batch["fuel_budget"]),
        "--output", output_dir,
    ]
    if batch["hyperparameters"] is not None:
        cmd += [
            "--hyperparameters", json.dumps(batch["hyperparameters"], separators=(",", ":")),
        ]
    if ptx_path is not None:
        cmd += ["--ptx", ptx_path]
    logger.debug("computing nonce: %s", " ".join(cmd[:4] + [f"'{cmd[4]}'"] + cmd[5:]))
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    while True:
        try:
            _, stderr = process.communicate(timeout=0.1)
            ret = process.returncode
            if not os.path.exists(output_file):
                if ret == 0:
                    raise Exception("no output")
                raise Exception(f"failed with exit code {ret}: {stderr}")

            start = now()
            cmd = [
                "docker", "exec", batch["challenge"], "tig-verifier",
                settings,
                batch["rand_hash"],
                str(nonce),
                output_file,
            ]
            if ptx_path is not None:
                cmd += ["--ptx", ptx_path]
            logger.debug("verifying nonce: %s", " ".join(cmd[:4] + [f"'{cmd[4]}'"] + cmd[5:]))
            ret = subprocess.run(cmd, capture_output=True, text=True)
            if ret.returncode != 0:
                raise Exception(
                    f"invalid solution (exit code: {ret.returncode}, stderr: {ret.stderr.strip()})"
                )

            last_line = ret.stdout.strip().splitlines()[-1]
            if not last_line.startswith("quality: "):
                raise Exception("failed to find quality in tig-verifier output")
            try:
                quality = int(last_line[len("quality: "):])
            except Exception:
                raise Exception("failed to parse quality from tig-verifier output")
            logger.debug(f"batch {batch['id']}, nonce {nonce} valid solution with quality {quality}")
            with open(output_file, "r") as f:
                d = json.load(f)
                d["quality"] = quality
            with open(output_file, "w") as f:
                json.dump(d, f)
            break
        except subprocess.TimeoutExpired:
            if batch["id"] not in PROCESSING_BATCH_IDS:
                process.kill()
                logger.debug(f"batch {batch['id']}, nonce {nonce} stopped")
                break

    logger.debug(f"batch {batch['id']}, nonce {nonce} finished, took {now() - start}ms")


def compute_merkle_roots(results_dir):
    for batch_id in list(PROCESSING_BATCH_IDS):
        job = PROCESSING_BATCH_IDS[batch_id]
        batch = job["batch"]
        start = job["start"]
        num_processing = batch["num_nonces"] - len(job["finished"])
        if num_processing > 0:
            logger.debug(f"batch {batch['id']} still processing {num_processing} nonces")
            time.sleep(1.5)
            continue

        try:
            solution_quality = []
            hashes = []
            for n in range(batch["start_nonce"], batch["start_nonce"] + batch["num_nonces"]):
                with open(f"{results_dir}/{batch['id']}/{n}.json", "r") as f:
                    d = json.load(f)
                    solution_quality.append(d.pop("quality"))
                    hashes.append(OutputData.from_dict(d).to_merkle_hash())

            merkle_tree = MerkleTree(hashes, batch["batch_size"])
            with open(f"{results_dir}/{batch['id']}/hashes.zlib", "wb") as f:
                hashes = [h.to_str() for h in hashes]
                f.write(zlib.compress(json.dumps(hashes).encode()))
            with open(f"{results_dir}/{batch['id']}/result.json", "w") as f:
                result = {
                    "solution_quality": solution_quality,
                    "merkle_root": merkle_tree.calc_merkle_root().to_str(),
                }
                json.dump(result, f)
            logger.info(f"batch {batch['id']} done, took: {now() - start}ms")
            _status_update_progress(batch["id"], batch["num_nonces"], batch["num_nonces"])
            READY_BATCH_IDS.add(batch["id"])
            _wake_poll()
        except Exception as e:
            logger.error(f"batch {batch['id']}, error computing merkle root : {e}")
            _status_push_recent(batch, "error", error=str(e))
        finally:
            PROCESSING_BATCH_IDS.pop(batch["id"], None)
            _mark_idle_if_quiet()
            return

    _mark_idle_if_quiet()
    time.sleep(1)


def purge_folders(output_path, ttl):
    n = now()
    # Only purge once both phases (if any) are past TTL.
    candidates = set(FINISHED_ROOT_BATCH_IDS) | set(FINISHED_PROOF_BATCH_IDS)
    purge_batch_ids = []
    for batch_id in candidates:
        root_t = FINISHED_ROOT_BATCH_IDS.get(batch_id)
        proof_t = FINISHED_PROOF_BATCH_IDS.get(batch_id)
        latest = max(t for t in (root_t, proof_t) if t is not None)
        if n >= latest + (ttl * 1000):
            purge_batch_ids.append(batch_id)
    if len(purge_batch_ids) == 0:
        time.sleep(5)
        return

    for batch_id in purge_batch_ids:
        if os.path.exists(f"{output_path}/{batch_id}"):
            logger.info(f"purging batch {batch_id}")
            shutil.rmtree(f"{output_path}/{batch_id}", ignore_errors=True)
        FINISHED_ROOT_BATCH_IDS.pop(batch_id, None)
        FINISHED_PROOF_BATCH_IDS.pop(batch_id, None)


def send_results(headers, master_ip, master_port, results_dir):
    try:
        batch_id = READY_BATCH_IDS.pop()
    except KeyError:
        logger.debug("no batches to send")
        _mark_idle_if_quiet()
        time.sleep(1)
        return

    output_folder = f"{results_dir}/{batch_id}"
    with open(f"{output_folder}/batch.json") as f:
        batch = json.load(f)
    with open(f"{output_folder}/result.json") as f:
        result = json.load(f)

    is_proof = batch.get("sampled_nonces") is not None
    finished_map = FINISHED_PROOF_BATCH_IDS if is_proof else FINISHED_ROOT_BATCH_IDS
    # Still inside post-submit cooldown: do not busy-spin re-queueing READY.
    if now() - finished_map.get(batch_id, 0) < 10000:
        READY_BATCH_IDS.add(batch_id)
        time.sleep(1)
        return

    def _on_submit_ok():
        # Only successful root/proof submits mark the phase finished.
        finished_map[batch_id] = now()
        _mark_idle_if_quiet()
        _wake_poll()

    if result.get("error") is not None:
        submit_url = f"http://{master_ip}:{master_port}/submit-batch-error/{batch_id}"
        logger.info(f"posting error for failed batch to {submit_url}")
        resp = requests.post(submit_url, headers=headers, json={"error": result["error"]}, timeout=60)
        if resp.status_code == 200:
            os.remove(f"{output_folder}/result.json")
            logger.info(f"successfully posted error for batch {batch_id}")
            # Do NOT mark phase finished — master typically clears slave and may
            # reassign the same batch for retry. Treating errors as finished left
            # max_concurrent=1 stuck with unpaid ready=NULL work.
            _status_clear_current(batch_id)
            _mark_idle_if_quiet()
            _wake_poll()
        elif resp.status_code == 408:
            logger.error(
                f"status {resp.status_code} when posting error for batch {batch_id} to master: {resp.text}"
            )
            # Keep result.json and retry; do not mark finished.
            READY_BATCH_IDS.add(batch_id)
            time.sleep(2)
        else:
            logger.error(f"status {resp.status_code} when posting error for batch {batch_id} to master: {resp.text}")
            READY_BATCH_IDS.add(batch_id)
            time.sleep(2)

    elif batch["sampled_nonces"] is None:
        submit_url = f"http://{master_ip}:{master_port}/submit-batch-root/{batch_id}"
        logger.info(f"posting root to {submit_url}")
        resp = requests.post(submit_url, headers=headers, json=result, timeout=60)
        if resp.status_code == 200:
            logger.info(f"successfully posted root for batch {batch_id}")
            _status_push_recent(batch, "submitted")
            _on_submit_ok()
        elif resp.status_code == 408:
            # Master raced the assignment away — keep result and retry. Do NOT
            # mark finished or we permanently drop unpaid work.
            logger.error(f"status {resp.status_code} when posting root for batch {batch_id} to master: {resp.text}")
            READY_BATCH_IDS.add(batch_id)
            time.sleep(2)
        else:
            logger.error(f"status {resp.status_code} when posting root for batch {batch_id} to master: {resp.text}")
            READY_BATCH_IDS.add(batch_id)
            time.sleep(2)

    else:
        with open(f"{output_folder}/hashes.zlib", "rb") as f:
            hashes = json.loads(zlib.decompress(f.read()).decode())
        leafs = []
        for n in batch["sampled_nonces"]:
            with open(f"{output_folder}/{n}.json", "r") as f:
                leafs.append(json.load(f))

        merkle_tree = MerkleTree(
            [MerkleHash.from_str(x) for x in hashes],
            batch["batch_size"],
        )
        proofs_to_submit = [
            dict(
                leaf=leaf,
                branch=merkle_tree.calc_merkle_branch(branch_idx=n - batch["start_nonce"]).to_str(),
            )
            for n, leaf in zip(batch["sampled_nonces"], leafs)
        ]

        submit_url = f"http://{master_ip}:{master_port}/submit-batch-proofs/{batch_id}"
        logger.info(f"posting proofs to {submit_url}")
        resp = requests.post(submit_url, headers=headers, json={"merkle_proofs": proofs_to_submit}, timeout=60)
        if resp.status_code == 200:
            logger.info(f"successfully posted proofs for batch {batch_id}")
            _status_push_recent(batch, "completed")
            _on_submit_ok()
        elif resp.status_code == 408:
            logger.error(f"status {resp.status_code} when posting proofs for batch {batch_id} to master: {resp.text}")
            READY_BATCH_IDS.add(batch_id)
            time.sleep(2)
        else:
            logger.error(f"status {resp.status_code} when posting proofs for batch {batch_id} to master: {resp.text}")
            READY_BATCH_IDS.add(batch_id)
            time.sleep(2)


def process_batch(algorithms_dir, results_dir):
    try:
        batch_id = PENDING_BATCH_IDS.pop()
    except KeyError:
        logger.debug("no pending batches")
        _mark_idle_if_quiet()
        time.sleep(1)
        return

    if batch_id in PROCESSING_BATCH_IDS or batch_id in READY_BATCH_IDS:
        return

    if os.path.exists(f"{results_dir}/{batch_id}/result.json"):
        logger.info(f"batch {batch_id} already processed")
        READY_BATCH_IDS.add(batch_id)
        _wake_poll()
        return

    with open(f"{results_dir}/{batch_id}/batch.json") as f:
        batch = json.load(f)

    containers = set(subprocess.check_output(["docker", "ps", "--format", "{{.Names}}"], text=True).splitlines())
    if batch["challenge"] not in containers:
        logger.error(
            f"Error processing batch {batch_id}: Challenge container {batch['challenge']} not found. "
            f"Did you start it with 'docker-compose up {batch['challenge']}'?"
        )
        return

    q = Queue()
    for n in range(batch["start_nonce"], batch["start_nonce"] + batch["num_nonces"]):
        q.put(n)
    _mark_busy()
    so_path, ptx_path = download_library(algorithms_dir, batch)
    started_ms = now()
    logger.info(f"batch {batch['id']} started")
    PROCESSING_BATCH_IDS[batch_id] = {
        "batch": batch,
        "so_path": so_path,
        "ptx_path": ptx_path,
        "q": q,
        "finished": set(),
        "start": started_ms,
    }
    _status_set_current(batch, nonces_done=0, started_ms=started_ms)


def process_nonces(results_dir):
    for batch_id in list(PROCESSING_BATCH_IDS):
        job = PROCESSING_BATCH_IDS[batch_id]
        q = job["q"]
        batch = job["batch"]
        so_path = job["so_path"]
        ptx_path = job["ptx_path"]
        try:
            nonce = q.get_nowait()
            break
        except Exception:
            continue
    else:
        time.sleep(1)
        return

    logger.debug(f"batch {batch_id}, nonce {nonce} started")
    try:
        run_tig_runtime(nonce, batch, so_path, ptx_path, results_dir)
        job["finished"].add(nonce)
        _status_update_progress(batch_id, len(job["finished"]), batch.get("num_nonces"))
    except Exception as e:
        msg = f"batch {batch_id}, nonce {nonce}, runtime error: {e}"
        logger.error(msg)
        with open(f"{results_dir}/{batch['id']}/result.json", "w") as f:
            json.dump({"error": msg}, f)
        READY_BATCH_IDS.add(batch["id"])
        PROCESSING_BATCH_IDS.pop(batch["id"], None)
        _status_push_recent(batch, "error", error=str(e))
        _wake_poll()


def poll_batches(headers, master_ip, master_port, results_dir, num_workers):
    get_batches_url = f"http://{master_ip}:{master_port}/get-batches"
    telem = _collect_host_telemetry(num_workers)
    req_headers = dict(headers)
    req_headers.update(_telemetry_headers(telem))
    logger.debug(
        "fetching batches from %s state=%s active=%s pending=%s last_idle_ms=%s version=%s",
        get_batches_url,
        telem.get("state"),
        telem.get("active_batches"),
        telem.get("pending_batches"),
        telem.get("last_idle_ms"),
        telem.get("slave_version"),
    )
    resp = requests.get(get_batches_url, headers=req_headers, params=telem, timeout=60)

    if resp.status_code == 200:
        batches = resp.json()
        root_batch_ids = [batch["id"] for batch in batches if batch["sampled_nonces"] is None]
        proofs_batch_ids = [batch["id"] for batch in batches if batch["sampled_nonces"] is not None]
        # Master can re-hand already-submitted batches while a concurrent slot is
        # stuck. Skip compute for those; re-queue submit after cooldown.
        # IMPORTANT: root and proof share the same id — only treat as stale when
        # that *phase* was already submitted.
        def _submitable_root_result(bid: str):
            result_path = f"{results_dir}/{bid}/result.json"
            if not os.path.exists(result_path):
                return False
            try:
                with open(result_path) as f:
                    result = json.load(f)
            except Exception:
                return False
            if result.get("error") is not None:
                return False
            return bool(result.get("merkle_root") and result.get("solution_quality") is not None)

        def _submitable_proof_result(bid: str):
            # Proofs are rebuilt from hashes.zlib + sampled nonce files.
            return os.path.exists(f"{results_dir}/{bid}/hashes.zlib")

        stale_roots = []
        for bid in root_batch_ids:
            if bid not in FINISHED_ROOT_BATCH_IDS:
                continue
            if _submitable_root_result(bid):
                stale_roots.append(bid)
            else:
                logger.warning(
                    "clearing finished root %s (no submitable result); will recompute",
                    bid,
                )
                FINISHED_ROOT_BATCH_IDS.pop(bid, None)

        stale_proofs = []
        for bid in proofs_batch_ids:
            if bid not in FINISHED_PROOF_BATCH_IDS:
                continue
            if _submitable_proof_result(bid):
                stale_proofs.append(bid)
            else:
                logger.warning(
                    "clearing finished proof %s (no hashes.zlib); will recompute",
                    bid,
                )
                FINISHED_PROOF_BATCH_IDS.pop(bid, None)

        stale = stale_roots + stale_proofs
        if stale:
            logger.debug(
                "master re-handed already-submitted batches: roots=%s proofs=%s",
                stale_roots,
                stale_proofs,
            )
            n = now()
            for bid in stale_roots:
                finished_at = FINISHED_ROOT_BATCH_IDS.get(bid, 0)
                if n - finished_at >= 10000:
                    READY_BATCH_IDS.add(bid)
            for bid in stale_proofs:
                # Ensure batch.json reflects proof phase before resubmit.
                finished_at = FINISHED_PROOF_BATCH_IDS.get(bid, 0)
                if n - finished_at >= 10000:
                    READY_BATCH_IDS.add(bid)
            root_batch_ids = [bid for bid in root_batch_ids if bid not in FINISHED_ROOT_BATCH_IDS]
            proofs_batch_ids = [bid for bid in proofs_batch_ids if bid not in FINISHED_PROOF_BATCH_IDS]
        if root_batch_ids or proofs_batch_ids:
            logger.info(f"root batches: {root_batch_ids}")
            logger.info(f"proofs batches: {proofs_batch_ids}")
        else:
            logger.debug(f"root batches: {root_batch_ids}")
            logger.debug(f"proofs batches: {proofs_batch_ids}")
        for batch in batches:
            bid = batch["id"]
            is_proof = batch.get("sampled_nonces") is not None
            if (is_proof and bid in FINISHED_PROOF_BATCH_IDS) or (
                (not is_proof) and bid in FINISHED_ROOT_BATCH_IDS
            ):
                continue
            output_folder = f"{results_dir}/{bid}"
            os.makedirs(output_folder, exist_ok=True)
            with open(f"{output_folder}/batch.json", "w") as f:
                json.dump(batch, f)
        PENDING_BATCH_IDS.clear()
        PENDING_BATCH_IDS.update(root_batch_ids + proofs_batch_ids)
        if PENDING_BATCH_IDS:
            _mark_busy()
        # If master only returned already-submitted ghosts, do NOT revoke real
        # in-flight work (that was killing live batches mid-nonce).
        if root_batch_ids or proofs_batch_ids:
            for batch_id in set(PROCESSING_BATCH_IDS) - set(root_batch_ids + proofs_batch_ids):
                logger.info(f"stopping batch {batch_id}")
                PROCESSING_BATCH_IDS.pop(batch_id, None)
        elif stale and PROCESSING_BATCH_IDS:
            logger.warning(
                "master returned only already-submitted batches %s; keeping in-flight %s",
                stale,
                sorted(PROCESSING_BATCH_IDS),
            )
        else:
            for batch_id in list(PROCESSING_BATCH_IDS):
                logger.info(f"stopping batch {batch_id}")
                PROCESSING_BATCH_IDS.pop(batch_id, None)
        _mark_idle_if_quiet()
        local_busy = bool(PENDING_BATCH_IDS or PROCESSING_BATCH_IDS or READY_BATCH_IDS)
        sleep_s = BUSY_POLL_SEC if local_busy else IDLE_POLL_SEC
    else:
        logger.error(f"status {resp.status_code} when fetching batch: {resp.text}")
        sleep_s = ERROR_POLL_SEC

    _POLL_WAKE.clear()
    if _POLL_WAKE.wait(timeout=max(0.05, sleep_s)):
        _POLL_WAKE.clear()
        logger.debug("poll woken early (submit/finish)")


def wrap_thread(func, *args):
    logger.info(f"Starting thread for {func.__name__}")
    while True:
        try:
            func(*args)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            time.sleep(5)


def main():
    global _SLAVE_NAME, _MASTER_LABEL, _NUM_WORKERS
    slave_name = os.getenv("SLAVE_NAME") or randomname.get_name()
    master_ip = os.getenv("MASTER_IP") or "0.0.0.0"
    if (master_port := os.getenv("MASTER_PORT")) is None:
        logger.error("MASTER_PORT environment variable not set")
        sys.exit(1)
    master_port = int(master_port)

    algorithms_dir = "algorithms"
    results_dir = "results"
    ttl = int(os.getenv("TTL"))
    num_workers = int(os.getenv("NUM_WORKERS"))
    _SLAVE_NAME = slave_name
    _MASTER_LABEL = f"{master_ip}:{master_port}"
    _NUM_WORKERS = num_workers

    print("Starting InnoPool slave with config:")
    print(f"  Slave Name: {slave_name}")
    print(f"  Slave Version: {SLAVE_VERSION}")
    print(f"  Master: {master_ip}:{master_port}")
    print(f"  CPU Architecture: {CPU_ARCH}")
    print(f"  Algorithms Dir: {algorithms_dir}")
    print(f"  Results Dir: {results_dir}")
    print(f"  TTL: {ttl}")
    print(f"  Workers: {num_workers}")
    print(f"  Idle poll: {IDLE_POLL_SEC}s  Busy poll: {BUSY_POLL_SEC}s")
    print(f"  Dashboard: http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")

    os.makedirs(algorithms_dir, exist_ok=True)
    _start_dashboard_server(num_workers)

    headers = {"User-Agent": slave_name}

    Thread(target=wrap_thread, args=(process_batch, algorithms_dir, results_dir)).start()
    for _ in range(num_workers):
        Thread(target=wrap_thread, args=(process_nonces, results_dir)).start()
    Thread(target=wrap_thread, args=(compute_merkle_roots, results_dir)).start()
    Thread(target=wrap_thread, args=(send_results, headers, master_ip, master_port, results_dir)).start()
    Thread(target=wrap_thread, args=(purge_folders, results_dir, ttl)).start()
    wrap_thread(poll_batches, headers, master_ip, master_port, results_dir, num_workers)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s - [%(name)s] - %(message)s",
        level=logging.DEBUG if os.getenv("VERBOSE") else logging.INFO,
    )
    main()
