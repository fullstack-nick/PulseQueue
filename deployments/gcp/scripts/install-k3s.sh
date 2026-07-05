#!/usr/bin/env bash
set -euo pipefail

sudo mkdir -p /etc/rancher/k3s
sudo tee /etc/rancher/k3s/config.yaml >/dev/null <<'YAML'
disable:
  - traefik
  - servicelb
write-kubeconfig-mode: "0644"
YAML

if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | sh -
else
  sudo systemctl enable k3s
  sudo systemctl restart k3s
fi

sudo systemctl is-active --quiet k3s
sudo k3s kubectl wait --for=condition=Ready node --all --timeout=180s

if ! command -v kubectl >/dev/null 2>&1; then
  sudo ln -sf /usr/local/bin/k3s /usr/local/bin/kubectl
fi

if ! command -v helm >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

kubectl version --client=true
helm version --short
kubectl get nodes -o wide
