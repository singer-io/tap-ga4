"""
Shared fixtures and utilities for tap-ga4 regression tests.
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REGRESSION_DIR = Path(__file__).parent
BASELINE_DIR = REGRESSION_DIR / "baseline"
TAP_CMD = str(Path(sys.executable).parent / "tap-ga4")

REQUIRED_ENV = [
    "TAP_GA4_OAUTH_CLIENT_ID",
    "TAP_GA4_OAUTH_CLIENT_SECRET",
    "TAP_GA4_REFRESH_TOKEN",
    "TAP_GA4_PROPERTY_ID",
    "TAP_GA4_ACCOUNT_ID",
    "TAP_GA4_REPORT_DEFINITIONS",
]


def build_config():
    missing = [v for v in REQUIRED_ENV if not os.getenv(v)]
    if missing:
        pytest.skip(f"Missing required env vars: {missing}")

    return {
        "oauth_client_id": os.environ["TAP_GA4_OAUTH_CLIENT_ID"],
        "oauth_client_secret": os.environ["TAP_GA4_OAUTH_CLIENT_SECRET"],
        "refresh_token": os.environ["TAP_GA4_REFRESH_TOKEN"],
        "property_id": os.environ["TAP_GA4_PROPERTY_ID"],
        "account_id": os.environ["TAP_GA4_ACCOUNT_ID"],
        "report_definitions": json.loads(os.environ["TAP_GA4_REPORT_DEFINITIONS"]),
        "start_date": os.getenv("TAP_GA4_START_DATE", "2026-05-01T00:00:00Z"),
    }


def _tap_env():
    env = os.environ.copy()
    env.setdefault("AES_SECRET_KEY", "peliqan-test-key")
    return env


def discover_catalog(config_path):
    """Run tap-ga4 in --discover mode and return the catalog dict."""
    result = subprocess.run(
        [TAP_CMD, "--config", config_path, "--discover"],
        stdout=subprocess.PIPE, stderr=sys.stderr, text=True, env=_tap_env()
    )
    if result.returncode != 0:
        raise RuntimeError(f"discover failed with exit code {result.returncode}")
    return json.loads(result.stdout)


def select_streams(catalog, stream_names=["test_report", "demographic_gender_report"]):
    """Mark only specific streams and their fields as selected."""
    for stream in catalog.get("streams", []):
        is_selected = stream.get("tap_stream_id") in stream_names
        for entry in stream.get("metadata", []):
            entry.setdefault("metadata", {})["selected"] = is_selected
    return catalog


def write_catalog(catalog):
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(catalog, tmp)
    tmp.close()
    return tmp.name


@pytest.fixture(scope="session")
def config_file():
    config = build_config()
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(config, tmp)
    tmp.close()
    yield tmp.name
    os.unlink(tmp.name)


@pytest.fixture(scope="session")
def catalog_file(config_file):
    catalog = discover_catalog(config_file)
    select_all_streams(catalog)
    path = write_catalog(catalog)
    yield path
    os.unlink(path)


def run_tap(config_path, catalog_path=None, extra_args=None):
    cmd = [TAP_CMD, "--config", config_path]
    if catalog_path:
        cmd += ["--catalog", catalog_path]
    cmd += (extra_args or [])
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, text=True, env=_tap_env())
    return result.stdout, "", result.returncode


def parse_messages(stdout):
    schemas, records, states = {}, {}, []
    for line in stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        t = msg.get("type")
        if t == "SCHEMA":
            schemas[msg["stream"]] = msg["schema"]
            records.setdefault(msg["stream"], [])
        elif t == "RECORD":
            records.setdefault(msg["stream"], []).append(msg["record"])
        elif t == "STATE":
            states.append(msg["value"])
    return schemas, records, states
