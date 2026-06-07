#!/usr/bin/env bash
set -euo pipefail

CDP_URL="${CDP_URL:-http://127.0.0.1:9222}"
OUT_PATH="${1:-${STORAGE_STATE_PATH:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/outputs/flow-storage-state.json}}"

python - "$CDP_URL" "$OUT_PATH" <<'PY'
import asyncio
import sys
from pathlib import Path

from playwright.async_api import async_playwright

cdp_url = sys.argv[1]
out_path = Path(sys.argv[2])

async def main() -> None:
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(cdp_url)
        if not browser.contexts:
            raise RuntimeError("No browser context found on CDP endpoint")
        context = browser.contexts[0]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(out_path))
        print(str(out_path))

asyncio.run(main())
PY
