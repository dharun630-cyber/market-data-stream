#!/usr/bin/env bash
# Bootstraps the always-on VM that runs Redpanda, the producer, and the
# consumer. Run this once on a fresh Ubuntu VM (a GCP free-tier e2-micro,
# or any small VPS) - these three need to run continuously, which is the
# wrong shape for a scale-to-zero platform like Cloud Run.
set -euo pipefail

curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"

git clone "${REPO_URL:?set REPO_URL to this repo}" app
cd app
docker compose up -d redpanda redis producer consumer
