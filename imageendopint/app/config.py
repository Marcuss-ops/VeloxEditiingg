from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _expand(path: str) -> Path:
    expanded = Path(path).expanduser()
    if not expanded.is_absolute():
        expanded = REPO_ROOT / expanded
    return expanded.resolve()


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
    redis_url: str
    redis_queue_name: str
    redis_job_key_prefix: str
    project_url_template: str
    prompt_selector: str | None
    submit_selector: str | None
    result_poll_seconds: int
    max_result_wait_seconds: int
    debug_screenshots: bool
    project_id_pool: list[str]


def load_settings() -> Settings:
    pool_str = os.getenv(
        "PROJECT_ID_POOL",
        "6a001474-4561-4f81-9c0d-65af18805fec,cb0cedf9-ba06-430f-ac7b-bd4342d2f03e,d169c946-60d3-4b75-b185-ec7e1db44a6f,e3053c71-7683-4110-aba3-95f43ac8acb7",
    )
    project_id_pool = [s.strip() for s in pool_str.split(",") if s.strip()]

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
            _expand(os.getenv("STORAGE_STATE_PATH", "outputs/flow-storage-state.json"))
            if os.getenv("STORAGE_STATE_PATH", "outputs/flow-storage-state.json")
            else None
        ),
        headless=_env_bool("HEADLESS", True),
        job_timeout_seconds=int(os.getenv("JOB_TIMEOUT_SECONDS", "900")),
        redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
        redis_queue_name=os.getenv("REDIS_QUEUE_NAME", "image-endpoint:jobs"),
        redis_job_key_prefix=os.getenv("REDIS_JOB_KEY_PREFIX", "image-endpoint:job"),
        project_url_template=os.getenv(
            "PROJECT_URL_TEMPLATE",
            "https://labs.google/fx/tools/flow/project/{project_id}",
        ),
        prompt_selector=os.getenv("PROMPT_SELECTOR") or None,
        submit_selector=os.getenv("SUBMIT_SELECTOR") or None,
        result_poll_seconds=int(os.getenv("RESULT_POLL_SECONDS", "5")),
        max_result_wait_seconds=int(os.getenv("MAX_RESULT_WAIT_SECONDS", "300")),
        debug_screenshots=_env_bool("DEBUG_SCREENSHOTS", False),
        project_id_pool=project_id_pool,
    )
