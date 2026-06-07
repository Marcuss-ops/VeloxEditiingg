from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _expand(path: str) -> Path:
    return Path(path).expanduser().resolve()


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    api_token: str | None
    chrome_executable: Path
    chrome_cdp_url: str | None
    profile_source_dir: Path
    profile_work_dir: Path
    cookie_jar_path: Path | None
    storage_state_path: Path | None
    headless: bool
    job_timeout_seconds: int
    project_url_template: str
    prompt_selector: str | None
    submit_selector: str | None
    result_poll_seconds: int
    max_result_wait_seconds: int


def load_settings() -> Settings:
    return Settings(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        api_token=os.getenv("API_TOKEN") or None,
        chrome_executable=_expand(
            os.getenv("CHROME_EXECUTABLE", "/opt/google/chrome/google-chrome")
        ),
        chrome_cdp_url=os.getenv("CHROME_CDP_URL") or None,
        profile_source_dir=_expand(
            os.getenv("PROFILE_SOURCE_DIR", "/home/pierone/.config/google-chrome")
        ),
        profile_work_dir=_expand(
            os.getenv("PROFILE_WORK_DIR", ".cache/google-chrome-headless")
        ),
        cookie_jar_path=(
            _expand(os.getenv("COOKIE_JAR_PATH"))
            if os.getenv("COOKIE_JAR_PATH")
            else None
        ),
        storage_state_path=(
            _expand(os.getenv("STORAGE_STATE_PATH"))
            if os.getenv("STORAGE_STATE_PATH")
            else None
        ),
        headless=_env_bool("HEADLESS", True),
        job_timeout_seconds=int(os.getenv("JOB_TIMEOUT_SECONDS", "900")),
        project_url_template=os.getenv(
            "PROJECT_URL_TEMPLATE",
            "https://labs.google/fx/tools/flow/project/{project_id}",
        ),
        prompt_selector=os.getenv("PROMPT_SELECTOR") or None,
        submit_selector=os.getenv("SUBMIT_SELECTOR") or None,
        result_poll_seconds=int(os.getenv("RESULT_POLL_SECONDS", "5")),
        max_result_wait_seconds=int(os.getenv("MAX_RESULT_WAIT_SECONDS", "300")),
    )
