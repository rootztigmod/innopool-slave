# InnoPool slave telemetry contract

Sent on every `GET /get-batches` (query params and/or `X-InnoPool-*` headers).
Stock slaves omit all fields; master degrades safely.

## Phase C — capacity (drives concurrent earn / load-shed)

| Field | Type | Notes |
|---|---|---|
| `cores` | int > 0 | Logical CPUs |
| `num_workers` | int > 0 | `NUM_WORKERS` |
| `load_1m` | float | 1-minute load average |
| `ram_gb` | int > 0 | Total RAM |
| `free_ram_gb` | float | Available RAM |
| `gpu_model` | string | GPU only (e.g. `NVIDIA GeForce RTX 4090`) |
| `gpu_util` | float | GPU only (0–100) |
| `gpu_vram_free_mb` | int > 0 | GPU only |

## v1.5 — runtime (observability / future scheduling)

| Field | Type | Notes |
|---|---|---|
| `state` | string | `idle` \| `downloading` \| `running` \| `submitting` |
| `active_batches` | int ≥ 0 | In `PROCESSING_BATCH_IDS` |
| `pending_batches` | int ≥ 0 | In `PENDING_BATCH_IDS` |
| `last_idle_ms` | int ≥ 0 | Last submit/finish → next local work gap; if currently idle, age of this idle |
| `slave_version` | string | e.g. `innopool-slave/0.1.0` |

## Idle reduction (slave behavior, not master)

1. Poll ~0.5–1s when idle / after empty get-batches  
2. Wake poll immediately after successful submit  
3. Back off ~2–5s when local queues are already full  

Master push-assign on submit is **not** required for v0.1.0.
