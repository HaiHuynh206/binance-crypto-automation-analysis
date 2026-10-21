#!/usr/bin/env bash
# Start (or reuse) a Cloudflare quick tunnel for the Airflow UI.
# The public URL is printed at the end. Ctrl-C does not stop it: the tunnel runs
# detached, use scripts/stop-share.sh to stop it.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p logs

CLOUDFLARED="${CLOUDFLARED:-$HOME/.local/bin/cloudflared}"
if [[ ! -x "$CLOUDFLARED" ]]; then
    echo "cloudflared not found at $CLOUDFLARED" >&2
    echo "Install: curl -fsSL -o ~/.local/bin/cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 && chmod +x ~/.local/bin/cloudflared" >&2
    exit 1
fi

if pgrep -x cloudflared >/dev/null; then
    echo "cloudflared is already running; reusing the existing tunnel."
else
    setsid nohup "$CLOUDFLARED" tunnel --url http://localhost:8080 \
        --no-autoupdate --logfile logs/cloudflared.log \
        > logs/cloudflared.out 2>&1 < /dev/null &
    echo $! > logs/cloudflared.pid
fi

for _ in $(seq 1 20); do
    url=$(grep -Eho 'https://[a-z0-9-]+\.trycloudflare\.com' \
        logs/cloudflared.out logs/cloudflared.log 2>/dev/null | head -1 || true)
    if [[ -n "$url" ]]; then
        echo "Airflow UI: $url"
        exit 0
    fi
    sleep 1
done

echo "Tunnel started but no URL found yet. Check logs/cloudflared.out" >&2
exit 1
