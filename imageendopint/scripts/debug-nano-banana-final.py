import asyncio
import re
import json
from pathlib import Path
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from app.config import load_settings
from app.browser.actions import _select_4_images_layout

async def debug_nano_banana():
    settings = load_settings()
    out_dir = Path("outputs/debug-nano-banana")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    project_id = "6a001474-4561-4f81-9c0d-65af18805fec"
    url = settings.project_url_template.format(project_id=project_id)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path=str(settings.chrome_executable),
            headless=settings.headless,
        )
        
        storage_state_path = Path("/home/pierone/Pyt/imageendopint/outputs/flow-storage-state.json")
        storage_state = str(storage_state_path) if storage_state_path.exists() else None
            
        context = await browser.new_context(
            storage_state=storage_state,
            viewport={"width": 1440, "height": 900},
        )
        
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        print("Navigating to page...")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        
        # Click Understood
        understood_btn = page.get_by_role("button", name=re.compile("Understood", re.I))
        if await understood_btn.count() > 0:
            await understood_btn.first.click()
            await page.wait_for_timeout(1000)

        # Use the updated modular function
        print("Attempting to select 'x4' layout...")
        success = await _select_4_images_layout(page)
        
        if success:
            print("Layout 'x4' selected successfully!")
            await page.screenshot(path=str(out_dir / "04-final-check-layout-x4.png"))
        else:
            print("Failed to select layout 'x4'. Check screenshots.")
            await page.screenshot(path=str(out_dir / "04-failed-layout.png"))
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_nano_banana())
