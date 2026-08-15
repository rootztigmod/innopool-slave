# innopool-slave

InnoPool’s custom TIG benchmarker slave: stock protocol, live telemetry, fast
re-poll, and a local status dashboard.

```bash
git clone https://github.com/rootztigmod/innopool-slave.git
cd innopool-slave
cp .env.example .env
# edit .env — at least SLAVE_NAME, NUM_WORKERS
mkdir -p data/algorithms data/results
docker compose up -d --build
```

Dashboard: [http://localhost:8787](http://localhost:8787)  
(Change `DASHBOARD_HOST_PORT` in `.env` if that port is taken.)

## What you get

| Piece | Purpose |
|---|---|
| `slave` | Custom worker (`main.py`) + dashboard on `:8787` |
| Challenge runtimes | `satisfiability`, `vehicle_routing`, `knapsack`, `job_scheduling`, `energy_arbitrage` |
| Telemetry | Sent on every `/get-batches` (see `TELEMETRY.md`) |

CPU challenges match the typical InnoPool `pool-cpu-*` route (`c001/c002/c003/c007/c008`).

GPU challenges (`vector_search`, `hypergraph`, `neuralnet_optimizer`) are in the same
compose file. Start only the services you need (Join-page `install.sh` does this for you):

```bash
# CPU only
docker compose up -d --build slave satisfiability vehicle_routing knapsack job_scheduling energy_arbitrage

# GPU only (needs NVIDIA Container Toolkit)
docker compose up -d --build slave vector_search hypergraph neuralnet_optimizer
```

Prefer the pool Join-page one-liner over manual clone when onboarding members.

## `.env` checklist

1. **`SLAVE_NAME`** — unique; must be allowed by the pool (e.g. `pool-cpu-<something>`).
2. **`MASTER_IP` / `MASTER_PORT`** — InnoPool master (defaults to `master.innopool.co.uk:80`).
3. **`NUM_WORKERS`** — roughly your vCPU count.
4. **`TIG_VERSION`** — TIG runtime image tag (`latest` or a pinned release).

## Useful commands

```bash
# All CPU services + slave
docker compose up -d --build

# Logs
docker compose logs -f slave

# Stop
docker compose down
```

Set `VERBOSE=1` in `.env` and recreate the slave for per-nonce debug logs.

## Utilization recorder (optional)

```bash
python3 record_util.py --hours 3 --interval 10
```

Writes `util_logs/util_*.jsonl` next to the compose project when run from this directory (requires the slave containers to be running).

## Drop-in mode (advanced)

If you already run stock `tig-benchmarker`, you can still copy `main.py` + `dashboard/` into `tig-benchmarker/slave/` instead of using this compose file. Prefer this repo’s `docker compose` flow for new installs.

## Version

See `VERSION` (currently `0.1.12`). Reported to the master as `innopool-slave/<VERSION>` from the packaged file — no `.env` override.

Stopping a batch kills `tig-runtime` / `tig-verifier` inside the challenge
container (not just the host `docker exec` client) and the slave stays
`running` until those processes are gone. A single empty `/get-batches` no
longer abandons in-flight work.

Missing challenge containers are reported to the master as infrastructure errors (so the
assignment is released) instead of silently re-queuing while the slave keeps heartbeating.
