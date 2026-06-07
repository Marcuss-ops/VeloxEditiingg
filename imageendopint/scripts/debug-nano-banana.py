import asyncio
import re
import json
from pathlib import Path
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from app.config import load_settings

async def debug_nano_banana():
    settings = load_settings()
    out_dir = Path("outputs/debug-nano-banana")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    project_id = "6a001474-4561-4f81-9c0d-65af18805fec"
    url = settings.project_url_template.format(project_id=project_id)
    
    print(f"Targeting URL: {url}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path=str(settings.chrome_executable),
            headless=settings.headless,
        )
        
        storage_state_path = Path("/home/pierone/Pyt/imageendopint/outputs/flow-storage-state.json")
        storage_state = str(storage_state_path) if storage_state_path.exists() else None
        print(f"Session path: {storage_state}")
            
        context = await browser.new_context(
            storage_state=storage_state,
            viewport={"width": 1440, "height": 900},
        )
        
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        print("Navigating to page...")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        
        # Initial screenshot
        await page.screenshot(path=str(out_dir / "01-initial.png"))

        # Step 0: Click 'Understood' to clear the view
        understood_btn = page.get_by_role("button", name=re.compile("Understood", re.I))
        if await understood_btn.count() > 0:
            print("Clicking 'Understood' banner...")
            await understood_btn.first.click()
            await page.wait_for_timeout(2000)
            await page.screenshot(path=str(out_dir / "02-after-cookies.png"))

        # Look for buttons to debug state
        buttons = await page.locator("button").all()
        print(f"Total buttons: {len(buttons)}")
        
        # Try to find and click Nano Banana
        # Using a more robust locator based on the specific text seen in logs
        agent_btn = page.locator("button").filter(has_text=re.compile(r"Nano Banana", re.I))
        if await agent_btn.count() == 0:
             agent_btn = page.locator("button").filter(has_text=re.compile(r"Agent", re.I))

        if await agent_btn.count() > 0:
            print("Found Nano Banana/Agent button, clicking...")
            await agent_btn.first.click()
            await page.wait_for_timeout(3000)
            await page.screenshot(path=str(out_dir / "03-after-banana-clicked.png"))
            
            # Dump DOM to analyze the popup
            dom = await page.content()
            (out_dir / "popup_dom.html").write_text(dom, encoding="utf-8")
            print(f"DOM dumped to {out_dir}/popup_dom.html")

            # Try to find all buttons in the popup area to list them
            print("Buttons currently visible:")
            popup_buttons = await page.locator("button").all()
            for i, b in enumerate(popup_buttons):
                try:
                    text = await b.inner_text()
                    is_vis = await b.is_visible()
                    if is_vis:
                        print(f" [{i}] Button: '{text.strip()}'")
                except: pass

            # Step 2: Click the '4' button in the layout popup
            # Trying multiple common selectors for that '4'
            layout_4 = page.locator("button").filter(has_text=re.compile(r"^4$", re.I))
            if await layout_4.count() > 0:
                print("Found '4' button via text, clicking...")
                await layout_4.first.click()
                await page.wait_for_timeout(2000)
                await page.screenshot(path=str(out_dir / "04-after-click-4.png"))
            else:
                print("Could not find '4' button via exact text. Keeping browser open for guidance.")
            
            # KEEP OPEN: Wait for a long time so user can guide
            print("BROWSER KEPT OPEN. You can check DOM and screenshots.")
            await page.wait_for_timeout(3600 * 1000) # 1 hour
        else:
            print("Nano Banana/Agent button not found.")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_nano_banana())
