from __future__ import annotations

import logging
import random
import re
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.responses import PlainTextResponse

from .config import Settings, load_settings
from .models import GenerateRequest
from .store import RedisJobStore


settings = load_settings()
app = FastAPI(title="Image Endpoint", version="0.1.0")
logger = logging.getLogger("image_endpoint.api")
job_store = RedisJobStore(
    redis_url=settings.redis_url,
    queue_name=settings.redis_queue_name,
    job_key_prefix=settings.redis_job_key_prefix,
)


def _require_token(authorization: str | None) -> None:
    if not settings.api_token:
        return
    expected = f"Bearer {settings.api_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _job_dir(job_id: str) -> Path:
    return Path("outputs") / job_id


def _client_ip(request: Request) -> str | None:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip() or None
    if request.client:
        return request.client.host
    return None


@app.get("/health")
async def health() -> dict[str, Any]:
    redis_status: dict[str, Any]
    try:
        redis_status = await job_store.healthcheck()
    except Exception as exc:  # noqa: BLE001
        redis_status = {"redis": False, "error": f"{type(exc).__name__}: {exc}"}

    return {
        "ok": True,
        "headless": settings.headless,
        "chrome_executable": str(settings.chrome_executable),
        "redis": redis_status,
    }


@app.on_event("startup")
async def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


@app.post("/v1/generate")
async def generate(
    payload: dict[str, Any],
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _require_token(authorization)

    project_id = payload.get("project_id")
    prompt = payload.get("prompt")
    if not project_id or not prompt:
        raise HTTPException(status_code=400, detail="project_id and prompt are required")

    # If project_id is a placeholder or not in our verified list, use round-robin
    if project_id == "velox-test" or str(project_id) not in settings.project_id_pool:
        # Atomic increment in Redis to pick the next project index
        # We access the redis client through the job_store
        rotation_key = f"{settings.redis_job_key_prefix}:rotation_index"
        idx = await job_store.redis.incr(rotation_key)
        project_id = settings.project_id_pool[idx % len(settings.project_id_pool)]
        logger.info("Assigned rotated project_id='%s' (index=%d) for this request", project_id, idx)

    req = GenerateRequest(
        project_id=str(project_id),
        prompt=str(prompt),
        negative_prompt=payload.get("negative_prompt"),
        reference_image_url=payload.get("reference_image_url"),
        webhook_url=payload.get("webhook_url"),
        extra={k: v for k, v in payload.items() if k not in {"project_id", "prompt", "negative_prompt", "reference_image_url", "webhook_url"}},
    )
    client_ip = _client_ip(request)
    user_agent = request.headers.get("user-agent")
    job = await job_store.create_job(req, client_ip=client_ip, user_agent=user_agent)
    await job_store.enqueue_job(job.id)
    logger.info("queued job_id=%s project_id=%s client_ip=%s", job.id, req.project_id, client_ip or "-")
    return {"job_id": job.id, "status": job.status}


@app.get("/v1/jobs/{job_id}")
async def job_status(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    _require_token(authorization)
    job = await job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_dict()


@app.get("/v1/jobs/{job_id}/log")
async def job_log(job_id: str, authorization: str | None = Header(default=None)) -> PlainTextResponse:
    _require_token(authorization)
    log_path = _job_dir(job_id) / "job.log"
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="log not found")
    return PlainTextResponse(log_path.read_text(encoding="utf-8", errors="replace"))


@app.get("/v1/jobs/{job_id}/artifact/{name}")
async def get_artifact(job_id: str, name: str, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    job = await job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    artifact_path = _job_dir(job_id) / name
    if not artifact_path.exists():
        raise HTTPException(status_code=404, detail="artifact not found")
    return FileResponse(artifact_path)
