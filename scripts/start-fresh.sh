#!/usr/bin/env bash
# Boot/start path: refresh images, then recreate containers.
# Do not start leftover containers from a crash — those layers can be torn
# and have taken down the slave (and upset the pool master).
set -euo pipefail

WTYPE="${1:-cpu}"
WTYPE="$(echo "$WTYPE" | tr '[:upper:]' '[:lower:]')"
DEST="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DEST"

if [[ "$WTYPE" == "gpu" ]]; then
  services="slave vector_search hypergraph neuralnet_optimizer"
else
  services="slave satisfiability vehicle_routing knapsack job_scheduling energy_arbitrage"
fi

if docker info >/dev/null 2>&1; then
  dcmd="docker"
else
  dcmd="sudo docker"
fi

if [[ -d .git ]]; then
  echo "==> Updating innopool-slave from git"
  git fetch --depth 1 origin "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)" || true
  git pull --ff-only || true
fi

echo "==> Pulling challenge runtime images"
# shellcheck disable=SC2086
$dcmd compose pull $services || true

echo "==> Recreating stack from pulled/built images"
# shellcheck disable=SC2086
$dcmd compose up -d --build --force-recreate --pull always $services

echo "==> InnoPool ${WTYPE} stack is up"
