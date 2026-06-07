from __future__ import annotations
import logging
import asyncio
import json
import random
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from .utils import _load_netscape_cookie_jar, _unique_preserve_order
from .extraction import _extract_image_sources, _download_images
from .actions import _find_editable, _click_by_text, _select_4_images_layout
from ..config import Settings

logger = logging.getLogger("image_endpoint.worker")


def _require_storage_state(settings: Settings) -> Path:
    if settings.storage_state_path is None:
        raise RuntimeError(
            "STORAGE_STATE_PATH is required. Export a valid Flow session first."
        )
    if not settings.storage_state_path.exists():
        raise FileNotFoundError(
            f"storage state file not found: {settings.storage_state_path}"
        )
    return settings.storage_state_path


async def run_generation(settings: Settings, request: Any, out_dir: Path) -> dict[str, Any]:
    # Random delay before starting to avoid saturation (1-5 seconds)
    start_delay = random.uniform(1.0, 5.0)
    logger.info("starting generation project_id=%s jitter=%.2fs", request.project_id, start_delay)
    await asyncio.sleep(start_delay)

    out_dir.mkdir(parents=True, exist_ok=True)

    url = settings.project_url_template.format(project_id=request.project_id)
    artifacts: list[dict[str, Any]] = []
    launched_browser = None

    async with async_playwright() as p:
        if settings.chrome_cdp_url:
            launched_browser = await p.chromium.connect_over_cdp(settings.chrome_cdp_url)
            if not launched_browser.contexts:
                raise RuntimeError("No Chrome context available on the CDP endpoint")
            context = launched_browser.contexts[0]
            auth_mode = "cdp"
        else:
            storage_state_path = _require_storage_state(settings)
            launched_browser = await p.chromium.launch(
                executable_path=str(settings.chrome_executable),
                headless=settings.headless,
                args=[
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--no-default-browser-check",
                ],
            )
            context = await launched_browser.new_context(
                storage_state=str(storage_state_path),
                viewport={"width": 1440, "height": 900},
            )
            auth_mode = f"storage_state:{storage_state_path}"
        logger.info("browser auth mode=%s", auth_mode)

        try:
            if settings.cookie_jar_path:
                cookies = _load_netscape_cookie_jar(settings.cookie_jar_path)
                if cookies:
                    await context.add_cookies(cookies)

            page = context.pages[0] if context.pages else await context.new_page()
            await Stealth().apply_stealth_async(page)
            page.set_default_timeout(60_000)

            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2_500)
            if settings.debug_screenshots:
                await page.screenshot(path=str(out_dir / "01-landing.png"), full_page=True)

            await _click_by_text(page, "Understood")
            await page.wait_for_timeout(1_000)
            
            if "labs.google/fx/tools/flow" in page.url and "accounts.google.com" not in page.url:
                await _click_by_text(page, "Try Google Flow")
                await page.wait_for_timeout(3_000)

            if "accounts.google.com" in page.url:
                raise RuntimeError(
                    f"Google sign-in required. The storage state file {settings.storage_state_path} did not contain a valid Flow session."
                )

            # --- Layout selection ---
            await _select_4_images_layout(page)
            if settings.debug_screenshots:
                await page.screenshot(path=str(out_dir / "02-after-layout-selection.png"), full_page=True)

            editable = await _find_editable(page, settings.prompt_selector)
            if editable is None:
                raise RuntimeError("No editable prompt field found")

            # Ensure focus and fill
            await editable.click()
            try:
                await editable.fill(request.prompt)
            except Exception:
                await editable.type(request.prompt, delay=20)

            await page.wait_for_timeout(500)
            if settings.debug_screenshots:
                await page.screenshot(path=str(out_dir / "03-prompt-filled.png"), full_page=True)

            await page.keyboard.press("Enter")
            await page.wait_for_timeout(1_500)
            if settings.debug_screenshots:
                await page.screenshot(path=str(out_dir / "04-after-submit.png"), full_page=True)
            
            # Capture existing sources immediately after Enter.
            existing_sources = _unique_preserve_order(await _extract_image_sources(page))

            # Poll for results
            max_wait = 80  # Force 80 seconds maximum wait
            poll_interval = 5.0 # 5 seconds as requested
            new_sources: list[str] = []
            took_40s_screenshot = False
            
            logger.info("starting 5s-interval polling loop for project_id=%s (max 80s)", request.project_id)
            start_poll_time = asyncio.get_event_loop().time()
            
            for i in range(int(max_wait / poll_interval)):
                elapsed = asyncio.get_event_loop().time() - start_poll_time
                
                # Take requested screenshot after 40 seconds
                if not took_40s_screenshot and elapsed >= 40:
                    if settings.debug_screenshots:
                        snap_path = out_dir / "05-after-submit-40s.png"
                        await page.screenshot(path=str(snap_path), full_page=True)
                        artifacts.append({"kind": "screenshot_40s", "name": snap_path.name})
                        logger.info("captured 40s mid-polling screenshot")
                    took_40s_screenshot = True

                # Extract and compare
                current_sources = _unique_preserve_order(await _extract_image_sources(page))
                new_sources = [s for s in current_sources if s not in existing_sources]
                
                # We expect 4 images, but we'll accept what we find if we hit the limit
                if len(new_sources) >= 4:
                    logger.info("found %d images after %.1fs", len(new_sources), elapsed)
                    break
                
                logger.info("polling... (elapsed %.1fs, found %d images)", elapsed, len(new_sources))
                await page.wait_for_timeout(int(poll_interval * 1000))

            logger.info("finished polling, final new_sources count=%d", len(new_sources))
            
            # Download new images
            logger.info("starting download of %d images", len(new_sources))
            downloaded_paths = await _download_images(context, new_sources, out_dir)
            logger.info("downloaded %d images successfully", len(downloaded_paths))

            screenshot_path = out_dir / "page.png"
            html_path = out_dir / "page.html"
            meta_path = out_dir / "result.json"

            if settings.debug_screenshots:
                await page.screenshot(path=str(screenshot_path), full_page=True)
            
            html_path.write_text(await page.content(), encoding="utf-8")

            # Store only filenames in artifacts for easier remote access
            if settings.debug_screenshots:
                artifacts.append({"kind": "screenshot", "name": screenshot_path.name})
            
            artifacts.append({"kind": "html", "name": html_path.name})
            
            # Filter for just JPGs and keep only the filenames
            final_filenames = [Path(p).name for p in downloaded_paths if p.lower().endswith(".jpg") or p.lower().endswith(".jpeg")]
            if not final_filenames:
                final_filenames = [Path(p).name for p in downloaded_paths]

            if final_filenames:
                artifacts.append({"kind": "downloaded_images", "value": final_filenames})

            meta = {
                "url": page.url,
                "title": await page.title(),
                "images": final_filenames,
                "artifacts": artifacts,
            }
            meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=True), encoding="utf-8")

            logger.info(
                "generation finished project_id=%s images=%d",
                request.project_id,
                len(final_filenames),
            )
            return meta
        finally:
            if not settings.chrome_cdp_url:
                await context.close()
                if launched_browser is not None:
                    await launched_browser.close()
