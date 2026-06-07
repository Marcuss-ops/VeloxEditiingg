from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

from .models import GenerateRequest, JobRecord, new_job


@dataclass(slots=True)
class RedisJobStore:
    redis_url: str
    queue_name: str = "image-endpoint:jobs"
    job_key_prefix: str = "image-endpoint:job"
    _redis: Redis | None = None

    def __post_init__(self) -> None:
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)

    def _job_key(self, job_id: str) -> str:
        return f"{self.job_key_prefix}:{job_id}"

    async def ping(self) -> bool:
        assert self._redis is not None
        return bool(await self._redis.ping())

    async def close(self) -> None:
        assert self._redis is not None
        await self._redis.close()

    async def create_job(
        self,
        request: GenerateRequest,
        *,
        client_ip: str | None = None,
        user_agent: str | None = None,
    ) -> JobRecord:
        job = new_job(request, client_ip=client_ip, user_agent=user_agent)
        await self.save_job(job)
        return job

    async def save_job(self, job: JobRecord) -> None:
        assert self._redis is not None
        await self._redis.set(self._job_key(job.id), json.dumps(job.to_dict(), ensure_ascii=False))

    async def enqueue_job(self, job_id: str) -> None:
        assert self._redis is not None
        await self._redis.rpush(self.queue_name, job_id)

    async def get_job(self, job_id: str) -> JobRecord | None:
        assert self._redis is not None
        raw = await self._redis.get(self._job_key(job_id))
        if not raw:
            return None
        return JobRecord.from_dict(json.loads(raw))

    async def update_job(self, job: JobRecord) -> None:
        await self.save_job(job)

    async def pop_next_job_id(self, timeout_seconds: int = 5) -> str | None:
        assert self._redis is not None
        item = await self._redis.blpop(self.queue_name, timeout=timeout_seconds)
        if not item:
            return None
        _, job_id = item
        return job_id

    async def healthcheck(self) -> dict[str, Any]:
        return {
            "redis": await self.ping(),
            "queue_name": self.queue_name,
            "job_key_prefix": self.job_key_prefix,
        }
