#!/usr/bin/env bash
# Migrate this project (code + optionally data) to a VPS and start the stack.
#
# Usage:
#   ./scripts/migrate-to-vps.sh root@1.2.3.4
#   ./scripts/migrate-to-vps.sh root@1.2.3.4 --with-data
#
# The remote host needs Ubuntu with SSH access and a sudo-capable user.
set -euo pipefail

usage() {
    sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
}

HOST="${1:-}"
if [[ -z "$HOST" || "$HOST" == "-h" || "$HOST" == "--help" ]]; then
    usage
    exit 1
fi
shift

WITH_DATA=0
for arg in "$@"; do
    case "$arg" in
        --with-data) WITH_DATA=1 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; usage; exit 1 ;;
    esac
done

LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE_DIR="bigdata-project"

for cmd in ssh rsync; do
    command -v "$cmd" >/dev/null || { echo "Missing command: $cmd" >&2; exit 1; }
done

echo "==> Creating ~/$REMOTE_DIR on $HOST"
ssh "$HOST" "mkdir -p ~/$REMOTE_DIR"

echo "==> Syncing code"
rsync -az --info=progress2 \
    --exclude '.venv/' \
    --exclude 'data/' \
    --exclude 'logs/' \
    --exclude '.env' \
    --exclude '__pycache__/' \
    --exclude '.pytest_cache/' \
    --exclude '.ruff_cache/' \
    --exclude '.git/' \
    "$LOCAL_DIR/" "$HOST:~/$REMOTE_DIR/"

if [[ "$WITH_DATA" -eq 1 ]]; then
    echo "==> Syncing data lake and completion markers"
    rsync -az --info=progress2 \
        "$LOCAL_DIR/data/" "$HOST:~/$REMOTE_DIR/data/"
else
    echo "==> Skipping data (fresh ingest on the VPS)"
fi

echo "==> Preparing .env"
ssh "$HOST" "cd ~/$REMOTE_DIR && { test -f .env || cp .env.example .env; }"

echo "==> Installing Docker (if needed) and starting the stack"
ssh -t "$HOST" "cd ~/$REMOTE_DIR && \
    { command -v docker >/dev/null || sudo bash scripts/install-docker.sh; } && \
    mkdir -p data && chmod 777 data && \
    sudo docker compose up -d --build"

echo "==> Unpausing the DAG"
ssh -t "$HOST" "cd ~/$REMOTE_DIR && \
    sudo docker compose exec airflow-scheduler airflow dags unpause crypto_market_ingest" || true

echo "==> Setting a random admin password"
ADMIN_PW="$(openssl rand -hex 12)"
ssh -t "$HOST" "cd ~/$REMOTE_DIR && \
    sudo docker compose exec airflow-scheduler airflow users reset-password -u admin -p '$ADMIN_PW'" || true

echo
echo "Admin password: $ADMIN_PW"
echo "Done. Check on the VPS:"
echo "  ssh $HOST 'cd ~/$REMOTE_DIR && sudo docker compose ps'"
echo "  ssh $HOST 'curl -s localhost:8080/health'"
echo
echo "Next: reset the admin password and set AIRFLOW_WEB_BIND=127.0.0.1 if you"
echo "only expose the UI through a tunnel. See docs/deploy-vps.md."
