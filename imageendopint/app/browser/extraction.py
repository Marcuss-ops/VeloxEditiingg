from __future__ import annotations
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import BrowserContext, Page
from .utils import _unique_preserve_order

async def _extract_image_sources(page: Page) -> list[str]:
    js = """
    () => {
      const sources = new Set();
      
      const walk = (root) => {
        root.querySelectorAll('img').forEach(img => {
          if (img.currentSrc) sources.add(img.currentSrc);
          if (img.src) sources.add(img.src);
          if (img.srcset) {
            img.srcset.split(',').forEach(s => {
              const url = s.trim().split(' ')[0];
              if (url) sources.add(url);
            });
          }
        });

        root.querySelectorAll('*').forEach(el => {
          const bg = window.getComputedStyle(el).backgroundImage;
          if (bg && bg !== 'none') {
            const match = bg.match(/url\\(["']?(.*?)["']?\\)/);
            if (match && match[1]) sources.add(match[1]);
          }
          if (el.shadowRoot) walk(el.shadowRoot);
        });
      };

      walk(document);
      return Array.from(sources).filter(s => s && s.startsWith('http'));
    }
    """
    try:
        return list(await page.evaluate(js))
    except Exception:
        return []

def _guess_extension(content_type: str | None, url: str) -> str:
    if content_type:
        ct = content_type.split(";", 1)[0].strip().lower()
        if ct == "image/png":
            return ".png"
        if ct in {"image/jpeg", "image/jpg"}:
            return ".jpg"
        if ct == "image/webp":
            return ".webp"
        if ct == "image/gif":
            return ".gif"
    path = urlparse(url).path.lower()
    for ext in [".png", ".jpg", ".jpeg", ".webp", ".gif"]:
        if path.endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".bin"

async def _download_images(context: BrowserContext, urls: list[str], out_dir: Path) -> list[str]:
    saved_paths: list[str] = []
    for index, url in enumerate(_unique_preserve_order(urls), start=1):
        try:
            response = await context.request.get(url)
            if not response.ok:
                continue
            ext = _guess_extension(response.headers.get("content-type"), url)
            path = out_dir / f"generated-{index:02d}{ext}"
            path.write_bytes(await response.body())
            saved_paths.append(str(path))
        except Exception:
            continue
    return saved_paths
