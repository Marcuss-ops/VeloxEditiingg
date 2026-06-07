from __future__ import annotations

import asyncio
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse

from .browser import run_generation
from .config import Settings, load_settings
from .models import GenerateRequest, JobRecord, new_job, utcnow


settings = load_settings()
app = FastAPI(title="Image Endpoint", version="0.1.0")

jobs: dict[str, JobRecord] = {}
job_lock = asyncio.Lock()


def _require_token(authorization: str | None) -> None:
    if not settings.api_token:
        return
    expected = f"Bearer {settings.api_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _job_dir(job_id: str) -> Path:
    return Path("outputs") / job_id


async def _execute_job(job: JobRecord) -> None:
    async with job_lock:
        job.status = "running"
        job.updated_at = utcnow()
        try:
            result = await asyncio.wait_for(
                run_generation(settings, job.request, _job_dir(job.id)),
                timeout=settings.job_timeout_seconds,
            )
            job.status = "succeeded"
            job.result = result
            job.updated_at = utcnow()
        except Exception as exc:  # noqa: BLE001
            job.status = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
            job.updated_at = utcnow()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "headless": settings.headless,
        "chrome_executable": str(settings.chrome_executable),
    }


@app.post("/v1/generate")
async def generate(
    payload: dict[str, Any],
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _require_token(authorization)

    project_id = payload.get("project_id")
    prompt = payload.get("prompt")
    if not project_id or not prompt:
        raise HTTPException(status_code=400, detail="project_id and prompt are required")

    req = GenerateRequest(
        project_id=str(project_id),
        prompt=str(prompt),
        negative_prompt=payload.get("negative_prompt"),
        reference_image_url=payload.get("reference_image_url"),
        extra={k: v for k, v in payload.items() if k not in {"project_id", "prompt", "negative_prompt", "reference_image_url"}},
    )
    job = new_job(req)
    jobs[job.id] = job

    background_tasks.add_task(_execute_job, job)
    return {"job_id": job.id, "status": job.status}


@app.get("/v1/jobs/{job_id}")
async def job_status(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    _require_token(authorization)
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_dict()


@app.get("/v1/jobs/{job_id}/artifact/{name}")
async def get_artifact(job_id: str, name: str, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    artifact_path = _job_dir(job_id) / name
    if not artifact_path.exists():
        raise HTTPException(status_code=404, detail="artifact not found")
    return FileResponse(artifact_path)
