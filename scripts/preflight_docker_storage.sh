#!/usr/bin/env bash
set -euo pipefail

# Preflight check for Docker Desktop VM free space before running SQL Server.
# This avoids SQL startup failures when Docker VM disk is exhausted.

DB_MAX_SIZE_MB="${DB_MAX_SIZE_MB:-100}"
DB_LOG_MAX_SIZE_MB="${DB_LOG_MAX_SIZE_MB:-10}"
DOCKER_HEADROOM_MB="${DOCKER_HEADROOM_MB:-2048}"
DOCKER_SPACE_PROBE_IMAGE="${DOCKER_SPACE_PROBE_IMAGE:-alpine:latest}"

for var in DB_MAX_SIZE_MB DB_LOG_MAX_SIZE_MB DOCKER_HEADROOM_MB; do
  if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
    echo "error: ${var} must be a non-negative integer (MB), got: ${!var}" >&2
    exit 2
  fi
done

required_mb=$((DB_MAX_SIZE_MB + DB_LOG_MAX_SIZE_MB + DOCKER_HEADROOM_MB))

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker command not found" >&2
  exit 2
fi

if ! docker info >/dev/null 2>&1; then
  echo "error: docker daemon is not reachable" >&2
  exit 2
fi

probe_df() {
  docker run --rm "${DOCKER_SPACE_PROBE_IMAGE}" sh -lc 'df -Pm / | awk "NR==2 {print \$2,\$3,\$4}"'
}

if ! out="$(probe_df 2>/dev/null)"; then
  echo "info: pulling ${DOCKER_SPACE_PROBE_IMAGE} for free-space probe..." >&2
  docker pull "${DOCKER_SPACE_PROBE_IMAGE}" >/dev/null
  out="$(probe_df)"
fi

total_mb="$(echo "${out}" | awk '{print $1}')"
used_mb="$(echo "${out}" | awk '{print $2}')"
free_mb="$(echo "${out}" | awk '{print $3}')"

if [[ -z "${total_mb}" || -z "${used_mb}" || -z "${free_mb}" ]]; then
  echo "error: failed to read Docker VM disk usage" >&2
  exit 2
fi

echo "Docker VM disk: total=${total_mb}MB used=${used_mb}MB free=${free_mb}MB"
echo "Configured SQL budget: data=${DB_MAX_SIZE_MB}MB log=${DB_LOG_MAX_SIZE_MB}MB"
echo "Required free MB (budget + headroom): ${required_mb}MB"

if (( free_mb < required_mb )); then
  cat >&2 <<EOF
error: insufficient Docker VM free space.
  free_mb=${free_mb}
  required_mb=${required_mb}

Increase Docker Desktop disk allocation and/or prune unused Docker artifacts,
then rerun this preflight before starting SQL Server.
EOF
  exit 1
fi

echo "ok: Docker VM free space is sufficient for configured SQL budget."
