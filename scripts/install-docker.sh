#!/usr/bin/env bash
# Install Docker Engine + Compose v2 on Ubuntu and allow the invoking user to
# use Docker without sudo. Run with: sudo bash scripts/install-docker.sh
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
    echo "This script must run as root (use sudo)." >&2
    exit 1
fi

TARGET_USER="${SUDO_USER:-hai-huynh}"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends docker.io docker-compose-v2

systemctl enable --now docker

if ! getent group docker >/dev/null; then
    groupadd docker
fi
usermod -aG docker "${TARGET_USER}"

docker --version
docker compose version
systemctl --no-pager --lines=5 status docker || true

echo
echo "Docker installed. User '${TARGET_USER}' added to group 'docker'."
echo "Log out and back in (or run 'newgrp docker') before using docker without sudo."
