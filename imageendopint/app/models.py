from __future__ import annotations

from dataclasses import dataclass, field
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
    webhook_url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "prompt": self.prompt,
            "negative_prompt": self.negative_prompt,
            "reference_image_url": self.reference_image_url,
            "webhook_url": self.webhook_url,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GenerateRequest":
        return cls(
            project_id=str(data["project_id"]),
            prompt=str(data["prompt"]),
            negative_prompt=data.get("negative_prompt"),
            reference_image_url=data.get("reference_image_url"),
            webhook_url=data.get("webhook_url"),
            extra=dict(data.get("extra") or {}),
        )


@dataclass
class JobRecord:
    id: str
    request: GenerateRequest
    client_ip: str | None = None
    user_agent: str | None = None
    status: str = "queued"
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    result: dict[str, Any] | None = None
    error: str | None = None

    def mark_running(self) -> None:
        self.status = "running"
        self.updated_at = utcnow()

    def mark_succeeded(self, result: dict[str, Any]) -> None:
        self.status = "succeeded"
        self.result = result
        self.error = None
        self.updated_at = utcnow()

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self.error = error
        self.updated_at = utcnow()

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "client_ip": self.client_ip,
            "user_agent": self.user_agent,
            "request": self.request.to_dict(),
            "result": self.result,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobRecord":
        return cls(
            id=str(data["id"]),
            request=GenerateRequest.from_dict(data["request"]),
            client_ip=data.get("client_ip"),
            user_agent=data.get("user_agent"),
            status=str(data.get("status", "queued")),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            result=data.get("result"),
            error=data.get("error"),
        )


def new_job(
    request: GenerateRequest,
    *,
    client_ip: str | None = None,
    user_agent: str | None = None,
) -> JobRecord:
    return JobRecord(id=str(uuid4()), request=request, client_ip=client_ip, user_agent=user_agent)
