#!/usr/bin/env bash
# Run baseline capture inside a Python 3.9 container.
# Usage: bash run_capture.sh

set -euo pipefail

TAP_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

echo "==> Running baseline capture in python:3.9-slim"
echo "    Tap dir: $TAP_DIR"

docker run --rm \
  -v "$TAP_DIR":/tap \
  -w /tap \
  -e TAP_GA4_OAUTH_CLIENT_ID="${TAP_GA4_OAUTH_CLIENT_ID:?}" \
  -e TAP_GA4_OAUTH_CLIENT_SECRET="${TAP_GA4_OAUTH_CLIENT_SECRET:?}" \
  -e TAP_GA4_REFRESH_TOKEN="${TAP_GA4_REFRESH_TOKEN:?}" \
  -e TAP_GA4_PROPERTY_ID="${TAP_GA4_PROPERTY_ID:?}" \
  -e TAP_GA4_ACCOUNT_ID="${TAP_GA4_ACCOUNT_ID:?}" \
  -e TAP_GA4_REPORT_DEFINITIONS="${TAP_GA4_REPORT_DEFINITIONS:?}" \
  -e TAP_GA4_START_DATE="${TAP_GA4_START_DATE:-2026-05-01T00:00:00Z}" \
  -e AES_SECRET_KEY="peliqan-test-key" \
  python:3.9-slim \
  bash -c "
    apt-get update -qq && apt-get install -y -qq git > /dev/null
    python -m venv /venv
    /venv/bin/pip install -e . -q
    /venv/bin/python tests/regression/capture.py
  "

echo ""
echo "==> Baseline written to tests/regression/baseline/"
