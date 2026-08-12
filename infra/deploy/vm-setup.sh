#!/usr/bin/env bash
set -euo pipefail

curl -fsSL https://get.docker.com | sudo sh

git clone "${REPO_URL:?set REPO_URL to this repo}" app
cd app
sudo REDIS_URL="${REDIS_URL:?set REDIS_URL to your Upstash Redis connection string}" \
  docker compose -f docker-compose.prod.yml up -d --build
