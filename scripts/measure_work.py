#!/usr/bin/env python3
"""Time one full machine on each challenge it can already run.

Run this on the slave host, from the innopool-slave directory, while it is
idle. The clock is this machine's clock. Pay is unchanged.

CPU: one nonce per logical processor, repeated until the run has lasted at
least 10 seconds. GPU: one nonce per GPU, same rule. Each challenge is
timed three times. The saved time is the average.

Writes measure_out/measure_<utc>.json and prints a table.
If the pool containers are not running, it starts a temporary runtime
container for each challenge that has an algorithm on disk.
"""

from __future__ import annotations

import io
import json
import os
import platform
import subprocess
import sys
import tarfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = os.environ.get("TIG_API_URL", "https://mainnet-api.tig.foundation").rstrip("/")
MIN_SECONDS = 10.0
MAX_WAVES = 30
RUNS = 3
SEED = "measure-work"

CPU = [
    ("satisfiability", "c001"),
    ("vehicle_routing", "c002"),
    ("knapsack", "c003"),
    ("job_scheduling", "c007"),
    ("energy_arbitrage", "c008"),
]
GPU = [
    ("vector_search", "c004"),
    ("hypergraph", "c005"),
    ("neuralnet_optimizer", "c006"),
]

# The algorithms, fuel budgets, and hyperparameters the pool actually assigns.
# A quality of 0 is a finished nonce. Fuel is per track, not one global cap.
POOL = {
    "vector_search": {
        "algorithm_id": "c004_a100",
        "tracks": {
            "n_queries=7000": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_queries=9000": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_queries=11000": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_queries=13000": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_queries=15000": {"fuel_budget": 5000000000000, "hyperparameters": None},
        },
    },
    "hypergraph": {
        "algorithm_id": "c005_a031",
        "tracks": {
            "n_h_edges=10000": {"fuel_budget": 50000000000000, "hyperparameters": {"effort": 5}},
            "n_h_edges=20000": {"fuel_budget": 50000000000000, "hyperparameters": {"effort": 5}},
            "n_h_edges=50000": {"fuel_budget": 50000000000000, "hyperparameters": {"effort": 0}},
            "n_h_edges=100000": {"fuel_budget": 50000000000000, "hyperparameters": {"effort": 0}},
            "n_h_edges=200000": {"fuel_budget": 50000000000000, "hyperparameters": {"effort": 0}},
        },
    },
    "neuralnet_optimizer": {
        "algorithm_id": "c006_a047",
        "tracks": {
            "n_hidden=4": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_hidden=7": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_hidden=10": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_hidden=14": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_hidden=18": {"fuel_budget": 5000000000000, "hyperparameters": None},
        },
    },
    "satisfiability": {
        "algorithm_id": "c001_a119",
        "tracks": {
            "n_vars=5000,ratio=4267": {"fuel_budget": 500000000000, "hyperparameters": None},
            "n_vars=7500,ratio=4267": {"fuel_budget": 500000000000, "hyperparameters": None},
            "n_vars=10000,ratio=4267": {"fuel_budget": 500000000000, "hyperparameters": None},
            "n_vars=100000,ratio=4150": {"fuel_budget": 500000000000, "hyperparameters": None},
            "n_vars=100000,ratio=4200": {"fuel_budget": 500000000000, "hyperparameters": None},
        },
    },
    "vehicle_routing": {
        "algorithm_id": "c002_a116",
        "tracks": {
            "n_nodes=600": {"fuel_budget": 5000000000000, "hyperparameters": {"allow_swap3": True, "granularity": 30, "granularity2": 40, "decomp_nb_phases": 8, "exploration_level": 4, "decomp_target_size": 200, "swapstar_capa_filter": 0.5, "max_credit_deterioration": 20}},
            "n_nodes=700": {"fuel_budget": 5000000000000, "hyperparameters": {"allow_swap3": True, "granularity": 30, "granularity2": 40, "decomp_nb_phases": 8, "exploration_level": 4, "decomp_target_size": 200, "swapstar_capa_filter": 0.5, "max_credit_deterioration": 20}},
            "n_nodes=800": {"fuel_budget": 5000000000000, "hyperparameters": {"allow_swap3": True, "granularity": 30, "granularity2": 40, "decomp_nb_phases": 12, "exploration_level": 4, "decomp_target_size": 200, "swapstar_capa_filter": 0.5, "max_credit_deterioration": 20}},
            "n_nodes=900": {"fuel_budget": 5000000000000, "hyperparameters": {"allow_swap3": True, "granularity": 30, "granularity2": 40, "decomp_nb_phases": 12, "exploration_level": 4, "decomp_target_size": 200, "swapstar_capa_filter": 0.5, "max_credit_deterioration": 20}},
            "n_nodes=1000": {"fuel_budget": 5000000000000, "hyperparameters": {"allow_swap3": True, "granularity": 30, "granularity2": 40, "decomp_nb_phases": 12, "exploration_level": 4, "decomp_target_size": 200, "swapstar_capa_filter": 0.5, "max_credit_deterioration": 20}},
        },
    },
    "knapsack": {
        "algorithm_id": "c003_a149",
        "tracks": {
            "n_items=1000,budget=5": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_items=1000,budget=10": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_items=1000,budget=25": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_items=5000,budget=10": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "n_items=5000,budget=25": {"fuel_budget": 5000000000000, "hyperparameters": None},
        },
    },
    "job_scheduling": {
        "algorithm_id": "c007_a040",
        "tracks": {
            "n=50,s=job_shop": {"fuel_budget": 150000000000, "hyperparameters": {"track": "job_shop", "job_shop_iters": 30000}},
            "n=50,s=fjsp_high": {"fuel_budget": 150000000000, "hyperparameters": {"track": "fjsp_high"}},
            "n=50,s=flow_shop": {"fuel_budget": 50000000000, "hyperparameters": {"track": "flow_shop"}},
            "n=50,s=fjsp_medium": {"fuel_budget": 150000000000, "hyperparameters": {"track": "fjsp_medium"}},
            "n=50,s=hybrid_flow_shop": {"fuel_budget": 150000000000, "hyperparameters": {"track": "hybrid_flow_shop"}},
        },
    },
    "energy_arbitrage": {
        "algorithm_id": "c008_a050",
        "tracks": {
            "s=dense": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "s=baseline": {"fuel_budget": 5000000000000, "hyperparameters": {"flow_margin": 0.05, "deflator_iters": 200, "use_cg_lp_combine": False, "use_pce_affine_recourse": True}},
            "s=capstone": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "s=multiday": {"fuel_budget": 5000000000000, "hyperparameters": None},
            "s=congested": {"fuel_budget": 5000000000000, "hyperparameters": {"soc_levels": 113, "action_grid": 40, "lp_total_pivots": 0, "network_derating": 0.4, "lns_lp_pivots_total": 16000, "dw_total_pivot_budget": 0}},
        },
    },
}

LIBRARY_STEM: dict[str, str] = {}


def _get(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "innopool-measure", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{url} returned {exc.code}") from exc


def pool_track(challenge: str) -> str:
    tracks = POOL[challenge]["tracks"]

    def size(track_id: str) -> tuple:
        nums = []
        for part in track_id.replace(",", "=").split("="):
            try:
                nums.append(int(part))
            except ValueError:
                continue
        return (nums[0] if nums else 10**12, track_id)

    return min(tracks, key=size)


def image_for(challenge: str) -> str:
    ret = subprocess.run(
        ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    suffix = f"/{challenge}/runtime:"
    found = [line.strip() for line in ret.stdout.splitlines() if suffix in line]
    for tag in ("0.0.8", "0.0.7"):
        for name in found:
            if name.endswith(":" + tag):
                return name
    if found:
        return found[0]
    pulled = f"ghcr.io/tig-foundation/tig-monorepo/{challenge}/runtime:0.0.8"
    print(f"pulling {pulled}")
    subprocess.run(["docker", "pull", pulled], check=True)
    return pulled


def ensure_container(challenge: str, gpus: bool) -> str:
    # Always a separate container. The pool's own challenge container stays
    # up and keeps its mount. This one mounts the downloaded pool libraries.
    name = f"measure-{challenge}"
    algo = algorithms_dir(gpus, challenge).resolve()
    if running(name) and name.startswith("measure-"):
        current = mounted_algorithms(name)
        if current and Path(current).resolve() != algo:
            subprocess.run(["docker", "rm", "-f", name], check=True)
    if running(name):
        return name
    out = Path(__file__).resolve().parent / "measure_out"
    out.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker", "run", "-d", "--name", name,
        "-v", f"{algo}:/app/algorithms",
        "-v", f"{out}:/tmp/measure",
    ]
    if gpus:
        cmd += ["--gpus", "all"]
    cmd += [image_for(challenge), "sleep", "infinity"]
    subprocess.run(cmd, check=True)
    return name


def mounted_algorithms(name: str) -> str | None:
    ret = subprocess.run(
        ["docker", "inspect", "-f", "{{range .Mounts}}{{.Destination}} {{.Source}}\n{{end}}", name],
        capture_output=True,
        text=True,
    )
    for line in ret.stdout.splitlines():
        dest, _, src = line.partition(" ")
        if dest == "/app/algorithms":
            return src
    return None


def running(name: str) -> bool:
    ret = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True,
        text=True,
    )
    return ret.returncode == 0 and ret.stdout.strip() == "true"


def busy(name: str) -> bool:
    ret = subprocess.run(
        ["docker", "exec", name, "pgrep", "-a", "tig-runtime"],
        capture_output=True,
        text=True,
    )
    return ret.returncode == 0 and bool(ret.stdout.strip())


def algorithms_dir(on_gpu: bool, challenge: str | None = None) -> Path:
    del on_gpu
    path = Path(__file__).resolve().parent / "measure_out" / "algorithms"
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_pool_library(challenge: str) -> str:
    algo_id = POOL[challenge]["algorithm_id"]
    root = algorithms_dir(False, challenge)
    arch = "arm64" if platform.machine() in {"aarch64", "arm64"} else "amd64"
    folder = root / challenge / arch
    existing = sorted(folder.glob("*.so")) if folder.is_dir() else []
    if challenge in LIBRARY_STEM:
        wanted = folder / f"{LIBRARY_STEM[challenge]}.so"
        if wanted.exists():
            return LIBRARY_STEM[challenge]
    if len(existing) == 1:
        LIBRARY_STEM[challenge] = existing[0].stem
        return existing[0].stem
    print(f"downloading {algo_id} for {challenge}")
    req = urllib.request.Request(
        f"{API}/get-binary-blob?algorithm_id={algo_id}",
        headers={"User-Agent": "innopool-measure"},
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        blob = resp.read()
    root.joinpath(challenge).mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        tar.extractall(root / challenge, filter="data")
    found = sorted(folder.glob("*.so"))
    if not found:
        raise FileNotFoundError(f"no {arch} library in {algo_id} blob")
    LIBRARY_STEM[challenge] = found[0].stem
    return found[0].stem


def find_library(challenge: str, on_gpu: bool = False) -> tuple[str, str | None]:
    stem = ensure_pool_library(challenge)
    arch = "arm64" if platform.machine() in {"aarch64", "arm64"} else "amd64"
    root = algorithms_dir(on_gpu, challenge)
    so = root / challenge / arch / f"{stem}.so"
    if not so.exists():
        raise FileNotFoundError(f"missing {so}")
    ptx = root / challenge / "ptx" / f"{stem}.ptx"
    if on_gpu and not ptx.exists():
        raise FileNotFoundError(f"no ptx for {stem} in {root / challenge / 'ptx'}")
    inside_so = f"/app/algorithms/{challenge}/{arch}/{so.name}"
    inside_ptx = f"/app/algorithms/{challenge}/ptx/{stem}.ptx" if ptx.exists() else None
    return inside_so, inside_ptx


def gpu_count() -> int:
    ret = subprocess.run(
        ["nvidia-smi", "-L"],
        capture_output=True,
        text=True,
    )
    if ret.returncode != 0:
        return 0
    return sum(1 for line in ret.stdout.splitlines() if line.startswith("GPU "))


def read_quality(container: str, settings: str, nonce: int, output_file: str, ptx: str | None) -> int | None:
    cmd = [
        "docker", "exec", container, "tig-verifier",
        settings, SEED, str(nonce), output_file,
    ]
    if ptx:
        cmd += ["--ptx", ptx]
    ret = subprocess.run(cmd, capture_output=True, text=True)
    if ret.returncode != 0:
        return None
    for line in reversed((ret.stdout or "").splitlines()):
        if line.startswith("quality: "):
            try:
                return int(line[len("quality: "):])
            except ValueError:
                return None
    return None


def run_challenge(container: str, challenge: str, challenge_id: str, workers: int, gpus: bool, track: str) -> dict:
    so, ptx = find_library(challenge, gpus)
    spec = POOL[challenge]["tracks"][track]
    fuel = str(int(spec["fuel_budget"]))
    hyper = spec["hyperparameters"]
    algo_id = POOL[challenge]["algorithm_id"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = f"/tmp/measure/{challenge}_{stamp}"
    host_out = Path(__file__).resolve().parent / "measure_out" / f"{challenge}_{stamp}"
    subprocess.run(["docker", "exec", container, "mkdir", "-p", out_dir], check=True)
    cmd_settings = {
        "algorithm_id": algo_id,
        "challenge_id": challenge_id,
        "track_id": track,
        "block_id": "",
        "player_id": "",
    }
    settings = json.dumps(cmd_settings, separators=(",", ":"))
    started = time.perf_counter()
    waves = 0
    failures = 0
    no_solution = 0
    solved = 0
    zero_quality = 0
    fuel_cutoff = 0
    first_error = ""
    nonce = 0
    while waves < MAX_WAVES and (time.perf_counter() - started) < MIN_SECONDS:
        procs = []
        for worker in range(workers):
            gpu = worker if gpus else None
            cmd = [
                "docker", "exec", container, "tig-runtime",
                settings, SEED, str(nonce), so,
                "--fuel", fuel,
                "--output", out_dir,
            ]
            if hyper is not None:
                cmd += ["--hyperparameters", json.dumps(hyper, separators=(",", ":"))]
            if ptx:
                cmd += ["--ptx", ptx]
            if gpu is not None:
                cmd += ["--gpu", str(gpu)]
            procs.append((nonce, subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)))
            nonce += 1
        for nonce_i, proc in procs:
            _, err = proc.communicate()
            code = proc.returncode
            output_file = f"{out_dir}/{nonce_i}.json"
            wrote = (host_out / f"{nonce_i}.json").exists()
            if wrote:
                quality = read_quality(container, settings, nonce_i, output_file, ptx)
                if quality is None:
                    failures += 1
                    if not first_error:
                        first_error = f"verifier rejected nonce {nonce_i} (runtime exit {code})"
                elif quality > 0:
                    solved += 1
                else:
                    zero_quality += 1
                continue
            if code == 85:
                no_solution += 1
            elif code == 87:
                fuel_cutoff += 1
                if not first_error:
                    first_error = "exit 87 with no solution file"
            else:
                failures += 1
                if not first_error:
                    lines = (err or "").strip().splitlines()
                    first_error = (lines[-1] if lines else f"exit {code}")[:400]
        waves += 1
        if failures == workers and waves == 1:
            break
    elapsed = time.perf_counter() - started
    return {
        "challenge": challenge,
        "algorithm_id": algo_id,
        "track_id": track,
        "fuel_budget": int(fuel),
        "workers": workers,
        "waves": waves,
        "nonces": nonce,
        "solved": solved,
        "zero_quality": zero_quality,
        "no_solution": no_solution,
        "fuel_cutoff": fuel_cutoff,
        "failures": failures,
        "first_error": first_error,
        "seconds": round(elapsed, 3),
        "library": so,
    }


def main() -> None:
    os.chdir(Path(__file__).resolve().parent)
    threads = os.cpu_count() or 1
    gpus = gpu_count()
    print(f"logical processors: {threads}")
    print(f"gpus: {gpus}")
    gpu_only = "--gpu-only" in sys.argv
    only = ""
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1]
    selected = GPU if gpu_only else CPU + GPU
    if only:
        selected = [pair for pair in selected if pair[0] == only]
    for name, cid in selected:
        try:
            find_library(name, name in {gpu for gpu, _cid in GPU})
        except FileNotFoundError:
            continue
        for track_id, spec in POOL[name]["tracks"].items():
            print(
                f"track {name}: {track_id}  {POOL[name]['algorithm_id']}  "
                f"fuel {spec['fuel_budget']}"
            )
    rows = []

    def measure(name: str, cid: str, workers: int, on_gpu: bool, track: str) -> None:
        try:
            find_library(name, on_gpu)
        except FileNotFoundError as exc:
            print(f"skip {name}: {exc}")
            return
        if running(name) and busy(name):
            raise SystemExit(f"{name} is already running tig-runtime. Stop the slave first.")
        print(f"starting runtime for {name}")
        container = ensure_container(name, on_gpu)
        print(f"measuring {name} {track} on {workers} {'GPUs' if on_gpu else 'processors'}...")
        runs = []
        for attempt in range(1, RUNS + 1):
            print(f"  run {attempt}/{RUNS}")
            run = run_challenge(container, name, cid, workers, on_gpu, track)
            runs.append(run)
            print(
                f"    {run['seconds']}s  solved {run['solved']}  "
                f"zero_quality {run['zero_quality']}  no_solution {run['no_solution']}  "
                f"fuel_cutoff {run['fuel_cutoff']}  failures {run['failures']}"
            )
        times = sorted(run["seconds"] for run in runs)
        median = times[len(times) // 2]
        mean = round(sum(times) / len(times), 3)
        chosen = next(run for run in runs if run["seconds"] == median)
        row = dict(chosen)
        row["seconds"] = mean
        row["median_seconds"] = median
        row["mean_seconds"] = mean
        row["run_seconds"] = [run["seconds"] for run in runs]
        rows.append(row)
        print(f"  average {mean}s  middle {median}s  runs {row['run_seconds']}")

    for name, cid in selected:
        on_gpu = name in {gpu for gpu, _cid in GPU}
        if on_gpu and gpus < 1:
            print(f"skip {name}: no GPU")
            continue
        for track in POOL[name]["tracks"]:
            measure(name, cid, gpus if on_gpu else threads, on_gpu, track)
    results = Path(__file__).resolve().parent / "measure_out"
    results.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = results / f"measure_{stamp}.json"
    path.write_text(json.dumps({"threads": threads, "gpus": gpus, "rows": rows}, indent=2))
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
