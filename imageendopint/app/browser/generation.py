from __future__ import annotations
import json
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from .utils import _copy_profile_tree, _load_netscape_cookie_jar, _unique_preserve_order
from .extraction import _extract_image_sources, _download_images
from .actions import _find_editable, _click_by_text, _select_4_images_layout
from ..config import Settings

async def run_generation(settings: Settings, request: Any, out_dir: Path) -> dict[str, Any]:
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
        elif settings.storage_state_path and settings.storage_state_path.exists():
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
                storage_state=str(settings.storage_state_path),
                viewport={"width": 1440, "height": 900},
            )
        else:
            profile_source = settings.profile_source_dir
            profile_work = settings.profile_work_dir
            _copy_profile_tree(profile_source, profile_work)
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(profile_work),
                executable_path=str(settings.chrome_executable),
                headless=settings.headless,
                args=[
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--no-default-browser-check",
                ],
                viewport={"width": 1440, "height": 900},
            )

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
            await page.screenshot(path=str(out_dir / "01-landing.png"), full_page=True)

            await _click_by_text(page, "Understood")
            await page.wait_for_timeout(1_000)
            
            if "labs.google/fx/tools/flow" in page.url and "accounts.google.com" not in page.url:
                await _click_by_text(page, "Try Google Flow")
                await page.wait_for_timeout(3_000)

            if "accounts.google.com" in page.url:
                raise RuntimeError(
                    "Google sign-in required. The cloned profile did not contain an active Flow session."
                )

            # --- Layout selection ---
            await _select_4_images_layout(page)
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
            await page.screenshot(path=str(out_dir / "03-prompt-filled.png"), full_page=True)

            await page.keyboard.press("Enter")
            
            # Capture existing sources immediately after Enter
            existing_sources = _unique_preserve_order(await _extract_image_sources(page))

            # Poll for results
            max_wait = 80
            poll_interval = 2.0 
            new_sources: list[str] = []
            
            for _ in range(int(max_wait / poll_interval)):
                current_sources = _unique_preserve_order(await _extract_image_sources(page))
                new_sources = [s for s in current_sources if s not in existing_sources]
                
                if len(new_sources) >= 4:
                    break
                await page.wait_for_timeout(int(poll_interval * 1000))

            screenshot_path = out_dir / "page.png"
            html_path = out_dir / "page.html"
            meta_path = out_dir / "result.json"
            
            # Download new images
            downloaded_paths = await _download_images(context, new_sources, out_dir)

            await page.screenshot(path=str(screenshot_path), full_page=True)
            html_path.write_text(await page.content(), encoding="utf-8")

            artifacts.append({"kind": "screenshot", "path": str(screenshot_path)})
            artifacts.append({"kind": "html", "path": str(html_path)})
            if new_sources:
                artifacts.append({"kind": "image_sources", "value": new_sources})
            if downloaded_paths:
                artifacts.append({"kind": "downloaded_images", "value": downloaded_paths})

            meta = {
                "url": page.url,
                "title": await page.title(),
                "image_sources": new_sources,
                "downloaded_images": downloaded_paths,
                "artifacts": artifacts,
            }
            meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=True), encoding="utf-8")

            return meta
        finally:
            if not settings.chrome_cdp_url:
                await context.close()
                if launched_browser is not None and not (
                    settings.storage_state_path and settings.storage_state_path.exists()
                ):
                    await launched_browser.close()
