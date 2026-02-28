.PHONY: test test-unit test-pipeline test-docker test-all test-ux test-dirty clean-test

# Fast unit tests (in-memory SQLite, no Docker, <30s)
test-unit:
	pytest tests/ -x -q -m "not pipeline" --tb=short

# Pipeline tests (needs ONNX model, <60s)
test-pipeline:
	pytest tests/ -x -q -m pipeline --tb=short

# Default: fast unit tests
test: test-unit

# Docker E2E — core suites (e2e + degraded, <5min with model cache)
test-docker:
	python tests/docker/run_all.py

# Everything including optional Docker suites
test-all:
	python tests/docker/run_all.py --suite all

# UX verification only
test-ux:
	python tests/docker/run_all.py --suite ux

# Dirty environment tests (GNU flex collision, conda, upgrade, minimal)
test-dirty:
	python tests/docker/run_all.py --suite dirty-devtools --suite dirty-conda --suite dirty-upgrade --suite dirty-minimal

# Clean test artifacts
clean-test:
	rm -rf /tmp/flex-test-results/
	docker rmi flex-test-e2e flex-test-install flex-test-upgrade flex-test-dirty-devtools flex-test-dirty-conda flex-test-dirty-upgrade flex-test-dirty-minimal 2>/dev/null || true
