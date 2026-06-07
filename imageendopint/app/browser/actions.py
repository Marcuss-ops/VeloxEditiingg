from __future__ import annotations
import logging
import re
from typing import Any
from playwright.async_api import Page

logger = logging.getLogger("image_endpoint.worker")

async def _find_editable(page: Page, selector: str | None) -> Any | None:
    if selector:
        locator = page.locator(selector)
        for idx in range(await locator.count()):
            candidate = locator.nth(idx)
            try:
                if await candidate.is_visible():
                    return candidate
            except Exception:
                continue

    candidates = [
        "[role='textbox']",
        "[contenteditable='true']",
        "input[type='text']",
        "input:not([type])",
        "textarea",
    ]
    for candidate in candidates:
        locator = page.locator(candidate)
        try:
            for idx in range(await locator.count()):
                item = locator.nth(idx)
                if await item.is_visible():
                    return item
        except Exception:
            continue
    return None

async def _click_submit(page: Page, selector: str | None) -> bool:
    if selector:
        locator = page.locator(selector)
        if await locator.count():
            await locator.first.click()
            return True

    button_names = ["Generate", "Create", "Run", "Submit", "Send"]
    for name in button_names:
        locator = page.get_by_role("button", name=re.compile(name, re.I))
        try:
            if await locator.count():
                await locator.first.click()
                return True
        except Exception:
            continue

    return False

async def _click_by_text(page: Page, text: str) -> bool:
    candidates = [
        page.get_by_role("button", name=re.compile(re.escape(text), re.I)),
        page.get_by_text(re.compile(rf"^{re.escape(text)}$", re.I)),
    ]
    for locator in candidates:
        try:
            if await locator.count():
                await locator.first.click()
                return True
        except Exception:
            continue
    return False

async def _select_4_images_layout(page: Page) -> bool:
    """
    Specifically for Flow: 
    1. Check if 'x4' is already active.
    2. If not, click the 'Nano Banana' pill to open settings.
    3. Click 'x4'.
    4. Close the popup to ensure prompt area is clear.
    """
    try:
        # Check if x4 is already visible and potentially active
        # (This avoids opening the menu if not needed)
        x4_btn = page.locator("button").filter(has_text=re.compile(r"^x4$", re.I))
        if await x4_btn.count() > 0:
            btn = x4_btn.first
            if await btn.is_visible():
                # Check if it's already "selected" (often has a specific class or aria attribute)
                # For now, let's just see if it's there. If it's visible, we might be in the popup.
                pass

        # Step 1: Click the agent/settings pill button
        agent_btn = page.locator("button").filter(has_text=re.compile(r"Nano Banana", re.I))
        count = await agent_btn.count()
        if count > 0:
            target = agent_btn.last # The pill in the bottom bar
            
            # Check if current text already says 'x4'
            current_text = await target.inner_text()
            if "x4" in current_text:
                logger.info("layout already set to x4 (detected in pill text)")
                return True

            logger.info("clicking layout settings pill (found %d candidates)", count)
            # Use shorter timeout to avoid 60s hang
            await target.click(timeout=10000)
            await page.wait_for_timeout(1000)
            
            # Step 2: Click the 'x4' button in the popup
            layout_x4 = page.locator("button").filter(has_text=re.compile(r"^x4$", re.I))
            
            success = False
            if await layout_x4.count() > 0:
                for i in range(await layout_x4.count()):
                    btn = layout_x4.nth(i)
                    if await btn.is_visible():
                        logger.info("selecting x4 layout")
                        await btn.click(timeout=5000)
                        await page.wait_for_timeout(500)
                        success = True
                        break
            
            if not success:
                layout_4 = page.locator("button").filter(has_text=re.compile(r"^4$", re.I))
                if await layout_4.count() > 0:
                     await layout_4.first.click(timeout=5000)
                     success = True
            
            # Step 3: CLOSE the popup by pressing Escape or clicking outside
            # This ensures the prompt field is not obscured
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)
            return success
                     
    except Exception as e:
        logger.warning("layout selection failed or timed out: %s", e)
        # Try to recover by closing any popup
        try: await page.keyboard.press("Escape")
        except: pass
    return False
