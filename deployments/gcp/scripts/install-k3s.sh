#!/usr/bin/env bash
set -euo pipefail

SWAP_FILE="/swapfile"
INSTALL_K3S_CHANNEL="${INSTALL_K3S_CHANNEL:-stable}"
if ! sudo swapon --show=NAME | grep -qx "$SWAP_FILE"; then
  if [ ! -f "$SWAP_FILE" ]; then
    sudo fallocate -l 1G "$SWAP_FILE"
    sudo chmod 600 "$SWAP_FILE"
    sudo mkswap "$SWAP_FILE"
  fi
  sudo swapon "$SWAP_FILE"
fi
if ! grep -qE "^$SWAP_FILE[[:space:]]" /etc/fstab; then
  echo "$SWAP_FILE none swap sw 0 0" | sudo tee -a /etc/fstab >/dev/null
fi

sudo mkdir -p /run/pulsequeue-k3s-db /var/lib/rancher/k3s/server
sudo chmod 700 /run/pulsequeue-k3s-db
if [ ! -e /var/lib/rancher/k3s/server/db ]; then
  sudo ln -s /run/pulsequeue-k3s-db /var/lib/rancher/k3s/server/db
fi

sudo mkdir -p /etc/rancher/k3s
sudo tee /etc/rancher/k3s/config.yaml >/dev/null <<'YAML'
disable:
  - traefik
  - servicelb
write-kubeconfig-mode: "0644"
YAML

if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL="$INSTALL_K3S_CHANNEL" sh -
else
  sudo systemctl enable k3s
  if ! sudo systemctl is-active --quiet k3s; then
    sudo systemctl start k3s
  fi
fi

sudo systemctl is-active --quiet k3s
node_seen=false
for _ in $(seq 1 60); do
  if sudo timeout 15s k3s kubectl get nodes --no-headers >/dev/null 2>&1; then
    node_seen=true
    break
  fi
  sleep 2
done
if [ "$node_seen" != "true" ]; then
  sudo timeout 30s k3s kubectl get nodes -o wide || true
  echo "k3s node was not discoverable within 120s" >&2
  exit 1
fi
sudo timeout 210s k3s kubectl wait --for=condition=Ready node --all --timeout=180s

if ! command -v kubectl >/dev/null 2>&1; then
  sudo ln -sf /usr/local/bin/k3s /usr/local/bin/kubectl
fi

if ! command -v helm >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

timeout 30s kubectl version --client=true
helm version --short
timeout 60s kubectl get nodes -o wide
