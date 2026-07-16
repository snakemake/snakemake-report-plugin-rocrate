#!/usr/bin/env bash
# Mimics the GitHub Actions CI pipeline locally.
# Run via: pixi run -e dev ci

set -euo pipefail

TEST_ENVS=(test-py311 test-py312 test-py313 test-py314)

echo "== formatting =="
pixi run -e dev format-check

echo "== linting =="
pixi run -e dev lint

echo "== typecheck =="
pixi run -e dev typecheck

echo "== build =="
pixi run -e dev check-build

for env in "${TEST_ENVS[@]}"; do
    echo "== verify-install ($env) =="
    pixi run -e "$env" verify-install

    echo "== test ($env) =="
    pixi run -e "$env" test
done

echo "All CI checks passed locally."