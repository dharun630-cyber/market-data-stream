#!/usr/bin/env bash
# Deploys the FastAPI serving layer to Cloud Run.
# Requires: gcloud CLI authenticated, a GCP project with billing enabled,
# and REDIS_URL pointing at your Upstash (or self-hosted) Redis instance.
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"  # keep this in a free-tier region
SERVICE_NAME="market-data-api"

gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  --project "${PROJECT_ID}"

gcloud builds submit --tag "gcr.io/${PROJECT_ID}/${SERVICE_NAME}" -f api/Dockerfile .

gcloud run deploy "${SERVICE_NAME}" \
  --image "gcr.io/${PROJECT_ID}/${SERVICE_NAME}" \
  --region "${REGION}" \
  --allow-unauthenticated \
  --set-env-vars "REDIS_URL=${REDIS_URL:?set REDIS_URL}" \
  --min-instances 0 \
  --max-instances 5
