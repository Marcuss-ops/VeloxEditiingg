"""bench — worker capacity certification harness modules.

Re-exports the most commonly used symbols so callers can write
``from bench import CapResult, dynamic_cap_search, choose_limit``.
"""

from bench.models import (
    CapResult,
    JobResult,
    DEFAULT_MAX_CAP,
    REQUIRED_METRICS,
    cap_matrix,
    now_ms,
    percentile,
)
from bench.http import (
    http_json,
    read_token,
    scrape_text,
    provision_m2m,
    delete_m2m,
)
from bench.metrics import (
    aggregate_gauge,
    delta,
    extract_observations,
    normalize_ratio,
    parse_prometheus,
    metric,
)
from bench.gates import (
    HARD_STOP_GATES,
    check_hard_stop_gates,
    hard_stops_passed,
    passes_safety_gates,
)
from bench.bottleneck import classify_bottleneck
from bench.search import dynamic_cap_search
from bench.sampling import Sampler
from bench.runner import (
    build_payload,
    command_for,
    render_command,
    run_cap_command,
    run_correctness_command,
    submit_and_poll,
    wait_cap,
)
from bench.report import choose_limit

__all__ = [
    # models
    "CapResult", "JobResult", "DEFAULT_MAX_CAP", "REQUIRED_METRICS",
    "cap_matrix", "now_ms", "percentile",
    # http
    "http_json", "read_token", "scrape_text", "provision_m2m", "delete_m2m",
    # metrics
    "aggregate_gauge", "delta", "extract_observations", "normalize_ratio",
    "parse_prometheus", "metric",
    # gates
    "HARD_STOP_GATES", "check_hard_stop_gates", "hard_stops_passed",
    "passes_safety_gates",
    # bottleneck
    "classify_bottleneck",
    # search
    "dynamic_cap_search",
    # sampling
    "Sampler",
    # runner
    "build_payload", "command_for", "render_command", "run_cap_command",
    "run_correctness_command", "submit_and_poll", "wait_cap",
    # report
    "choose_limit",
]
