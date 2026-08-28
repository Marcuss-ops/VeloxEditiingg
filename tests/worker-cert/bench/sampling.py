"""Background sampler for Prometheus metrics during a cap run."""

from __future__ import annotations

import threading
from typing import Any

from bench.http import scrape_text
from bench.metrics import extract_observations, parse_prometheus


class Sampler:
    """Background thread that periodically scrapes Prometheus endpoints."""

    def __init__(self, urls: list[str], token: str, interval_s: float) -> None:
        self.urls = urls
        self.token = token
        self.interval_s = interval_s
        self.samples: list[dict[str, float | None]] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def finish(self) -> None:
        self.stop.set()
        self.thread.join(timeout=max(2.0, self.interval_s * 2))

    def _run(self) -> None:
        while not self.stop.is_set():
            merged: dict[str, float] = {}
            for url in self.urls:
                for name, value in parse_prometheus(scrape_text(url, self.token)).items():
                    merged[name] = merged.get(name, 0.0) + value
            if merged:
                self.samples.append(extract_observations(merged))
            self.stop.wait(self.interval_s)
