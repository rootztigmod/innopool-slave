#!/usr/bin/env bash
# Boot/start path: refresh images, then recreate containers.
# Do not start leftover containers from a crash — those layers can be torn
# and have taken down the slave (and upset the pool master).
set -euo pipefail

WTYPE="${1:-cpu}"
WTYPE="$(echo "$WTYPE" | tr '[:upper:]' '[:lower:]')"
DEST="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DEST"
chmod +x "$DEST/scripts/start-fresh.sh" 2>/dev/null || true

if [[ "$WTYPE" == "gpu" ]]; then
  services="slave vector_search hypergraph neuralnet_optimizer"
  pull_services="vector_search hypergraph neuralnet_optimizer"
else
  services="slave satisfiability vehicle_routing knapsack job_scheduling energy_arbitrage"
  pull_services="satisfiability vehicle_routing knapsack job_scheduling energy_arbitrage"
fi

if docker info >/dev/null 2>&1; then
  dcmd="docker"
else
  dcmd="sudo docker"
fi

with_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

sync_repo() {
  [[ -d .git ]] || return 0
  echo "==> Updating innopool-slave from git"
  local branch env_bak=""
  branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)"
  if [[ -f .env ]]; then
    env_bak="$(mktemp)"
    cp .env "$env_bak"
  fi
  git fetch --depth 1 origin "$branch" || true
  if ! git pull --ff-only; then
    echo "Local branch diverged from origin/${branch}; resetting (keeping .env)"
    git reset --hard "origin/${branch}" || true
  fi
  if [[ -n "$env_bak" && -f "$env_bak" ]]; then
    cp "$env_bak" .env
    rm -f "$env_bak"
  fi
}

enable_boot_unit() {
  local unit="innopool-slave-${WTYPE}.service"
  local start_script="${DEST}/scripts/start-fresh.sh"
  echo "==> Installing ${unit}"
  if ! with_root tee "/etc/systemd/system/${unit}" >/dev/null <<EOF
[Unit]
Description=InnoPool ${WTYPE} slave (pull images, then start)
After=docker.service network-online.target
Wants=network-online.target
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${DEST}
TimeoutStartSec=900
ExecStart=${start_script} ${WTYPE}
ExecStop=/bin/bash -lc 'cd ${DEST} && (docker compose stop || sudo docker compose stop)'

[Install]
WantedBy=multi-user.target
EOF
  then
    echo "Warning: could not write ${unit}."
    echo "  sudo systemctl enable ${unit}"
    return 0
  fi
  with_root systemctl daemon-reload || true
  with_root systemctl enable "$unit" || true
  echo "Enabled ${unit} (pull + recreate on boot)."
}

sync_repo

echo "==> Pulling challenge runtime images"
# shellcheck disable=SC2086
$dcmd compose pull $pull_services || true

echo "==> Recreating stack from pulled/built images"
# shellcheck disable=SC2086
$dcmd compose up -d --build --force-recreate --pull missing $services

enable_boot_unit

echo "==> InnoPool ${WTYPE} stack is up"
