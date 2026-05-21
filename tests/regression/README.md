# tap-ga4 — Python 3.11 Regression Tests

Before/after parity test for the Python 3.9 → 3.11 migration.

## Strategy

1. **Capture** baseline on pre-migration `master` + Python 3.9: discover, select all streams, sync, record schemas / state-key names / record field names.
2. **Compare** on migration branch + Python 3.11 — assert schemas unchanged, no streams dropped, all baseline fields still present.

## Files

| File | Purpose |
| --- | --- |
| `conftest.py` | Shared fixtures: config builder, discover + select_all, sync runner, message parser |
| `capture.py` | Standalone — run once on Python 3.9 to write `baseline/` |
| `test_regression.py` | pytest suite — run on Python 3.11 to verify parity |
| `run_capture.sh` | Wrapper that runs `capture.py` in `python:3.9-slim` Docker |
| `run_tests.sh` | Wrapper that runs pytest in `python:3.11-slim` Docker |
| `baseline/` | Committed reference output: schemas, state keys, record fields, catalog, meta |

## Usage

Required env vars:

```bash
export TAP_GA4_OAUTH_CLIENT_ID=...
export TAP_GA4_OAUTH_CLIENT_SECRET=...
export TAP_GA4_REFRESH_TOKEN=...   # OAuth refresh token with analytics.readonly scope
export TAP_GA4_PROPERTY_ID=...     # the GA4 property ID
export TAP_GA4_ACCOUNT_ID=...      # the GA4 account ID
export TAP_GA4_REPORT_DEFINITIONS='[{"name": "test_report", "id": "test_report", "dimensions": ["date"], "metrics": ["sessions"]}]' # JSON string with report definitions
export TAP_GA4_START_DATE=2010-01-01T00:00:00Z   # optional
```

### Capture baseline (pre-migration code on Python 3.9)

```bash
git worktree add /tmp/tap-ga4-master master
cp -r tests/regression /tmp/tap-ga4-master/tests/regression
cd /tmp/tap-ga4-master
bash tests/regression/run_capture.sh
cp -r tests/regression/baseline /path/to/migration-branch/tests/regression/
git worktree remove /tmp/tap-ga4-master
```

### Run tests (migration branch on Python 3.11)

```bash
bash tests/regression/run_tests.sh
```

## Notes

- The OAuth client used for tap-ga4 is **not** the SSO client — it lives in a separate Google Cloud project (`1013877377144`) which has the Google Analytics Data API enabled. The SSO project doesn't have Google Analytics Data API turned on.
- `prompt=consent` + `access_type=offline` are required in the authorize URL to mint a refresh_token. Without `prompt=consent`, Google won't re-issue a refresh_token on re-auth.
