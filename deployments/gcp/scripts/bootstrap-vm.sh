#!/usr/bin/env bash
set -euo pipefail

sudo mkdir -p /opt/pulsequeue
sudo chown -R "$USER":"$USER" /opt/pulsequeue
if [ -d "$HOME/.docker" ]; then
  sudo chown -R "$USER":"$USER" "$HOME/.docker" || true
fi

if command -v docker >/dev/null 2>&1; then
  sudo systemctl enable --now docker
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  docker --version
  docker compose version
  exit 0
fi

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
  curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
fi

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
if command -v docker >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-plugin
else
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
sudo usermod -aG docker "$USER"
sudo systemctl enable --now docker
docker --version
docker compose version
