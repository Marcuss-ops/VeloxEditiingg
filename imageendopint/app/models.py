from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class GenerateRequest:
    project_id: str
    prompt: str
    negative_prompt: str | None = None
    reference_image_url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobRecord:
    id: str
    request: GenerateRequest
    status: str = "queued"
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    result: dict[str, Any] | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "request": {
                "project_id": self.request.project_id,
                "prompt": self.request.prompt,
                "negative_prompt": self.request.negative_prompt,
                "reference_image_url": self.request.reference_image_url,
                "extra": self.request.extra,
            },
            "result": self.result,
            "error": self.error,
        }


def new_job(request: GenerateRequest) -> JobRecord:
    return JobRecord(id=str(uuid4()), request=request)

