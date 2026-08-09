const STATE_LABELS = {
  idle: "Waiting",
  downloading: "Downloading",
  running: "Benchmarking",
  submitting: "Submitting",
};

function $(id) {
  return document.getElementById(id);
}

function fmtMs(ms) {
  if (ms == null || Number.isNaN(ms)) return "—";
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function shortId(id) {
  if (!id) return "—";
  if (id.length <= 18) return id;
  return `${id.slice(0, 8)}…${id.slice(-6)}`;
}

function render(data) {
  $("slave-name").textContent = data.slave_name || "—";
  $("version").textContent = data.version || "—";
  $("master").textContent = data.master || "—";

  const state = data.state || "idle";
  const chip = $("state-chip");
  chip.textContent = STATE_LABELS[state] || state;
  chip.className = `state-chip ${state}`;

  const gpuModel = data.gpu_model;
  if (gpuModel) {
    $("hw-label").textContent = "GPU";
    $("cores").textContent = gpuModel;
    $("cores").classList.add("mono");
  } else {
    $("hw-label").textContent = "Cores";
    $("cores").textContent = data.cores ?? "—";
    $("cores").classList.remove("mono");
  }
  $("workers").textContent = data.num_workers ?? "—";

  const gpuUtil = Number(data.gpu_util);
  if (gpuModel && Number.isFinite(gpuUtil)) {
    $("load-label").textContent = "GPU util";
    $("load-bar").style.width = `${Math.min(100, Math.max(0, gpuUtil))}%`;
    const vramTotal = Number(data.gpu_vram_total_mb);
    const vramUsed = Number(data.gpu_vram_used_mb);
    if (Number.isFinite(vramTotal) && Number.isFinite(vramUsed) && vramTotal > 0) {
      $("load-text").textContent =
        `${gpuUtil.toFixed(0)}% · VRAM ${Math.round(vramUsed)} / ${Math.round(vramTotal)} MB`;
    } else {
      $("load-text").textContent = `${gpuUtil.toFixed(0)}%`;
    }
  } else {
    $("load-label").textContent = "Load";
    const cores = Math.max(1, Number(data.cores) || 1);
    const load = Number(data.load_1m);
    const loadPct = Number.isFinite(load) ? Math.min(100, (load / cores) * 100) : 0;
    $("load-bar").style.width = `${loadPct}%`;
    $("load-text").textContent = Number.isFinite(load)
      ? `${load.toFixed(2)} (${(load / cores).toFixed(2)}/core)`
      : "—";
  }

  const ramGb = data.ram_gb;
  const freeGb = data.free_ram_gb;
  $("ram").textContent =
    freeGb != null && ramGb != null ? `${freeGb} / ${ramGb} GB` : "—";
  $("idle").textContent = fmtMs(data.last_idle_ms);
  $("updated").textContent = `Updated ${new Date().toLocaleTimeString()}`;

  const cur = data.current;
  const empty = $("current-empty");
  const card = $("current-card");
  if (!cur) {
    empty.classList.remove("hidden");
    card.classList.add("hidden");
  } else {
    empty.classList.add("hidden");
    card.classList.remove("hidden");
    const challenge =
      cur.challenge_id || cur.challenge || "—";
    const challengeLabel = cur.challenge_id && cur.challenge
      ? `${cur.challenge_id} · ${cur.challenge}`
      : challenge;
    $("cur-challenge").textContent = challengeLabel;
    $("cur-phase").textContent = cur.phase || "—";
    $("cur-algo").textContent = cur.algorithm_id || cur.algorithm || "—";
    $("cur-track").textContent = cur.track_id || "—";
    const done = cur.nonces_done ?? 0;
    const total = cur.nonces_total ?? 0;
    $("cur-nonces").textContent = total ? `${done} / ${total}` : "—";
    $("cur-batch").textContent = shortId(cur.batch_id);
    const pct = total ? Math.min(100, (done / total) * 100) : 0;
    $("nonce-bar").style.width = `${pct}%`;
    $("cur-elapsed").textContent = `Elapsed ${fmtMs(cur.elapsed_ms)}`;
  }

  const list = $("recent-list");
  const recentEmpty = $("recent-empty");
  const recent = Array.isArray(data.recent) ? data.recent : [];
  list.innerHTML = "";
  if (!recent.length) {
    recentEmpty.classList.remove("hidden");
  } else {
    recentEmpty.classList.add("hidden");
    for (const item of recent) {
      const li = document.createElement("li");
      const badge = document.createElement("span");
      badge.className = `badge ${item.status || "completed"}`;
      badge.textContent = item.status || "completed";

      const main = document.createElement("div");
      main.className = "recent-main";
      const title = document.createElement("strong");
      const ch = item.challenge_id || item.challenge || "batch";
      const algo = item.algorithm_id || item.algorithm || "";
      title.textContent = algo ? `${ch} · ${algo}` : ch;
      const detail = document.createElement("span");
      detail.textContent = item.error
        ? `${shortId(item.batch_id)} — ${item.error}`
        : shortId(item.batch_id);
      main.appendChild(title);
      main.appendChild(detail);

      const time = document.createElement("span");
      time.className = "recent-time";
      time.textContent = item.ts_ms
        ? new Date(item.ts_ms).toLocaleTimeString()
        : "";

      li.appendChild(badge);
      li.appendChild(main);
      li.appendChild(time);
      list.appendChild(li);
    }
  }
}

async function tick() {
  try {
    const resp = await fetch("/api/status", { cache: "no-store" });
    if (!resp.ok) throw new Error(`status ${resp.status}`);
    render(await resp.json());
  } catch (err) {
    $("updated").textContent = `Update failed: ${err.message}`;
  }
}

tick();
setInterval(tick, 1000);
