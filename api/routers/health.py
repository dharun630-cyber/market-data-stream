from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

router = APIRouter(tags=["ops"])

REQUEST_COUNTER = Counter("api_requests_total", "Total API requests", ["path", "status"])


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
