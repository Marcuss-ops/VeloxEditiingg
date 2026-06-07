from __future__ import annotations
import re
from typing import Any
from playwright.async_api import Page

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
    1. Look for the 'Nano Banana' pill button at the bottom
    2. Click it to open the settings popup
    3. Look for the 'x4' button specifically and click it
    """
    try:
        # Step 1: Click the agent/settings pill button
        # In the screenshot it has text like "Nano Banana 2 x2"
        agent_btn = page.locator("button").filter(has_text=re.compile(r"Nano Banana", re.I))
        
        if await agent_btn.count() > 0:
            print("Clicking Nano Banana settings pill...")
            await agent_btn.first.click()
            await page.wait_for_timeout(1500)
            
            # Step 2: Click the 'x4' button in the popup
            # We look for a button that has EXACTLY 'x4' as text to avoid confusion
            # Or use a more specific selector if available
            layout_x4 = page.locator("button").filter(has_text=re.compile(r"^x4$", re.I))
            
            if await layout_x4.count() > 0:
                # Make sure we click the one that is visible (the one in the popup)
                for i in range(await layout_x4.count()):
                    btn = layout_x4.nth(i)
                    if await btn.is_visible():
                        print("Found 'x4' button in popup, clicking...")
                        await btn.click()
                        await page.wait_for_timeout(1000)
                        return True
            else:
                # Fallback: try just '4' if 'x4' is not found
                layout_4 = page.locator("button").filter(has_text=re.compile(r"^4$", re.I))
                if await layout_4.count() > 0:
                     await layout_4.first.click()
                     return True
                     
    except Exception as e:
        print(f"Error in _select_4_images_layout: {e}")
    return False
