#!/usr/bin/env bash
# Stop the Cloudflare quick tunnel started by scripts/share-ui.sh.
set -euo pipefail

if pgrep -x cloudflared >/dev/null; then
    pkill -x cloudflared
    echo "cloudflared stopped."
else
    echo "cloudflared is not running."
fi

cd "$(dirname "$0")/.."
rm -f logs/cloudflared.pid
