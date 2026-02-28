#!/usr/bin/env python3
"""
Unified Docker test runner for flex.

Usage:
    python tests/docker/run_all.py                    # core suites (e2e + degraded)
    python tests/docker/run_all.py --suite e2e        # single suite
    python tests/docker/run_all.py --suite all        # everything including optional
    python tests/docker/run_all.py --skip upgrade     # skip specific
    python tests/docker/run_all.py --parallel         # concurrent execution
    python tests/docker/run_all.py --no-build         # skip Docker build
    python tests/docker/run_all.py --json report.json # custom report path

Environment:
    FLEX_MODEL_CACHE     Path to model cache (default: ~/.flex/models)
    ANTHROPIC_API_KEY    For agent-driven UX tests (optional)
"""
import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DOCKER_DIR = Path(__file__).resolve().parent
RESULTS_DIR = Path("/tmp/flex-test-results")

# ── Suite definitions ─────────────────────────────────────────────────────────

SUITES = {
    "e2e": {
        "dockerfile": "Dockerfile.e2e",
        "runner": "/run_e2e.py",
        "image": "flex-test-e2e",
        "timeout": 300,
        "needs_rw_model": False,
    },
    "degraded": {
        "dockerfile": "Dockerfile.e2e",
        "runner": "/run_install_degraded.py",
        "image": "flex-test-e2e",
        "timeout": 300,
        "needs_rw_model": True,   # must corrupt model — no ro mount
    },
    "install": {
        "dockerfile": "Dockerfile.install",
        "runner": "/run_install.py",
        "image": "flex-test-install",
        "timeout": 300,
        "needs_rw_model": False,
    },
    "ux": {
        "dockerfile": "Dockerfile.e2e",
        "runner": "/run_ux.py",
        "image": "flex-test-e2e",
        "timeout": 180,
        "needs_rw_model": False,
    },
    "upgrade": {
        "dockerfile": "Dockerfile.upgrade",
        "runner": "/run_upgrade.py",
        "image": "flex-test-upgrade",
        "timeout": 300,
        "needs_rw_model": False,
    },
    "dirty-devtools": {
        "dockerfile": "Dockerfile.dirty-devtools",
        "runner": "/run_dirty.py --scenario devtools",
        "image": "flex-test-dirty-devtools",
        "timeout": 300,
        "needs_rw_model": False,
    },
    "dirty-conda": {
        "dockerfile": "Dockerfile.dirty-conda",
        "runner": "/run_dirty.py --scenario conda",
        "image": "flex-test-dirty-conda",
        "timeout": 300,
        "needs_rw_model": False,
    },
    "dirty-upgrade": {
        "dockerfile": "Dockerfile.dirty-upgrade",
        "runner": "/run_dirty.py --scenario upgrade",
        "image": "flex-test-dirty-upgrade",
        "timeout": 300,
        "needs_rw_model": False,
    },
    "dirty-minimal": {
        "dockerfile": "Dockerfile.dirty-minimal",
        "runner": "/run_dirty.py --scenario minimal",
        "image": "flex-test-dirty-minimal",
        "timeout": 300,
        "needs_rw_model": False,
    },
}

# Default suites (fast, deterministic)
DEFAULT_SUITES = ["e2e", "degraded"]


def _model_cache() -> Path:
    return Path(os.environ.get("FLEX_MODEL_CACHE", Path.home() / ".flex" / "models"))


def _model_mount_args(suite: dict) -> list[str]:
    """Mount model cache if available and suite allows ro mount."""
    if suite["needs_rw_model"]:
        return []
    cache = _model_cache()
    if cache.exists() and (cache / "model.onnx").exists():
        return ["-v", f"{cache}:/root/.flex/models:ro"]
    return []


def _build_image(dockerfile: str, image: str) -> tuple[bool, str]:
    """Build Docker image. Returns (success, error_output)."""
    r = subprocess.run(
        ["docker", "build",
         "-f", str(DOCKER_DIR / dockerfile),
         "-t", image,
         str(REPO_ROOT)],
        capture_output=True, text=True, timeout=300,
    )
    return r.returncode == 0, r.stderr[-500:] if r.returncode != 0 else ""


def _run_suite(name: str, suite: dict) -> dict:
    """Run a test suite in Docker. Returns result dict."""
    t0 = time.time()

    # Create per-suite results dir on host
    suite_dir = RESULTS_DIR / name
    suite_dir.mkdir(parents=True, exist_ok=True)

    model_args = _model_mount_args(suite)

    runner_parts = suite["runner"].split()
    cmd = [
        "docker", "run", "--rm",
        *model_args,
        "-v", f"{suite_dir}:/tmp:rw",
        suite["image"],
        "python3", *runner_parts,
    ]

    r = subprocess.run(
        cmd,
        timeout=suite["timeout"],
        capture_output=False,  # stream output live
    )
    elapsed = time.time() - t0

    # Read JSON result
    json_files = list(suite_dir.glob("flex-test-*.json"))
    suite_json = None
    if json_files:
        try:
            suite_json = json.loads(json_files[0].read_text())
        except Exception:
            pass

    return {
        "name": name,
        "exit_code": r.returncode,
        "elapsed_s": round(elapsed, 1),
        "json": suite_json,
    }


def main():
    parser = argparse.ArgumentParser(description="Flex Docker test runner")
    parser.add_argument("--suite", action="append",
                        help="Run specific suite(s). Use 'all' for everything.")
    parser.add_argument("--skip", action="append", default=[],
                        help="Skip specific suite(s)")
    parser.add_argument("--json", default=str(RESULTS_DIR / "report.json"),
                        help="Combined JSON report path")
    parser.add_argument("--parallel", action="store_true",
                        help="Run independent suites concurrently")
    parser.add_argument("--no-build", action="store_true",
                        help="Skip Docker build (use existing images)")
    args = parser.parse_args()

    # Determine suites
    if args.suite and "all" in args.suite:
        selected = list(SUITES.keys())
    elif args.suite:
        selected = args.suite
    else:
        selected = DEFAULT_SUITES

    selected = [s for s in selected if s in SUITES and s not in args.skip]

    # Filter unavailable optional suites
    runnable = []
    for name in selected:
        suite = SUITES[name]
        dockerfile = DOCKER_DIR / suite["dockerfile"]
        if not dockerfile.exists():
            print(f"  [skip] {name} ({suite['dockerfile']} not found)")
            continue
        runnable.append(name)

    if not runnable:
        print("No suites to run.")
        sys.exit(0)

    print(f"\n  Suites: {', '.join(runnable)}\n")

    # ── Build phase ───────────────────────────────────────────────────────────
    if not args.no_build:
        seen = {}
        for name in runnable:
            s = SUITES[name]
            key = s["dockerfile"]
            if key not in seen:
                seen[key] = s["image"]
                print(f"  Building {s['image']} ({key})...", end=" ", flush=True)
                ok, err = _build_image(key, s["image"])
                print("ok" if ok else f"FAILED\n{err}")
                if not ok:
                    sys.exit(2)
        print()

    # ── Run phase ─────────────────────────────────────────────────────────────
    results = []
    t_all = time.time()

    if args.parallel and len(runnable) > 1:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(_run_suite, name, SUITES[name]): name
                for name in runnable
            }
            for future in as_completed(futures):
                r = future.result()
                status = "\033[32mPASS\033[0m" if r["exit_code"] == 0 else "\033[31mFAIL\033[0m"
                print(f"\n  [{status}] {r['name']} ({r['elapsed_s']}s)")
                results.append(r)
    else:
        for name in runnable:
            print(f"{'='*60}")
            print(f"  Suite: {name}")
            print(f"{'='*60}")
            r = _run_suite(name, SUITES[name])
            status = "\033[32mPASS\033[0m" if r["exit_code"] == 0 else "\033[31mFAIL\033[0m"
            print(f"\n  [{status}] {name} ({r['elapsed_s']}s)\n")
            results.append(r)

    total_elapsed = time.time() - t_all

    # ── Combined report ───────────────────────────────────────────────────────
    total_pass = total_fail = 0
    for r in results:
        if r["json"]:
            total_pass += r["json"].get("passed", 0)
            total_fail += r["json"].get("failed", 0)
        elif r["exit_code"] != 0:
            total_fail += 1

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "elapsed_s": round(total_elapsed, 2),
        "total_passed": total_pass,
        "total_failed": total_fail,
        "overall_result": "PASS" if total_fail == 0 else "FAIL",
        "suites": {
            r["name"]: r["json"] if r["json"] else {
                "suite": r["name"],
                "exit_code": r["exit_code"],
                "elapsed_s": r["elapsed_s"],
                "error": "no JSON output",
            }
            for r in results
        },
    }

    report_path = Path(args.json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    suite_summary = "  ".join(
        f"{'ok' if r['exit_code'] == 0 else 'FAIL'} {r['name']}"
        for r in results
    )
    print(f"  {suite_summary}")
    print(f"  {total_pass} passed, {total_fail} failed ({total_elapsed:.1f}s)")
    print(f"  Report: {report_path}")

    if total_fail > 0:
        print("\n\033[31m  OVERALL: FAIL\033[0m")
        sys.exit(1)
    else:
        print("\n\033[32m  OVERALL: PASS\033[0m")
        sys.exit(0)


if __name__ == "__main__":
    main()
