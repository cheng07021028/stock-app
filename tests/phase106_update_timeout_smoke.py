from pathlib import Path
import importlib.util
import sys
import time
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_request_budget():
    off = load("official_factor_service")
    off._begin_run_budget(1, 2)
    assert off._consume_request("t") <= 5
    assert off._consume_request("t") <= 5
    try:
        off._consume_request("t")
        raise AssertionError("request budget did not stop")
    except off.OfficialFactorBudgetExceeded:
        pass
    status = off._end_run_budget()
    assert status["request_count"] == 2


def test_official_quick_mode_stops_and_preserves_universe():
    off = load("official_factor_service")
    original_get = off.requests.get

    def fail(*args, **kwargs):
        time.sleep(0.10)
        raise requests.exceptions.Timeout("simulated timeout")

    off.requests.get = fail
    try:
        started = time.monotonic()
        df, meta = off.build_official_factor_cache(
            save=False,
            quick_mode=True,
            max_runtime_seconds=2,
            max_requests=5,
            finmind_bulk_only=True,
        )
        elapsed = time.monotonic() - started
    finally:
        off.requests.get = original_get
    assert elapsed < 6, elapsed
    assert int(meta.get("request_count", 0)) <= 5
    assert bool(meta.get("timed_out"))
    assert len(df) >= 1000  # previous valid cache/universe remains usable


def test_macro_executor_does_not_wait_forever():
    macro = load("macro_startup_service")
    original = macro._fetch_yahoo_chart

    def slow(*args, **kwargs):
        time.sleep(2)
        return {"ok": False, "error": "slow"}

    macro._fetch_yahoo_chart = slow
    try:
        started = time.monotonic()
        result = macro._run_fast_update(sync_github=False, max_runtime_seconds=10)
        elapsed = time.monotonic() - started
    finally:
        macro._fetch_yahoo_chart = original
    assert elapsed < 20, elapsed
    assert isinstance(result, dict)


def main():
    test_request_budget()
    test_official_quick_mode_stops_and_preserves_universe()
    test_macro_executor_does_not_wait_forever()
    print("PASS phase106 bounded update smoke")


if __name__ == "__main__":
    main()
