# Atriveo Logo Sync

Standalone repository for syncing company logos into Cloudflare R2 using [LogoHunter](https://github.com/koodaamo/logohunter).

This repo is intentionally separate from the Atriveo application codebase.

## What this does

- Reads company targets from:
  - `data/companies.csv`, or
  - Atriveo API (`/api/jobs`) when `ATRIVEO_API_URL` + `ATRIVEO_API_TOKEN` are set.
- Fetches logos with LogoHunter.
- Uploads logos to Cloudflare R2.
- Writes a manifest:
  - `output/logos.json`
  - `output/last-run.json`
- Runs on GitHub Actions:
  - scheduled daily
  - manual trigger
  - optional `repository_dispatch` trigger

## Repository structure

- `scripts/sync_logos.py` main sync script
- `.github/workflows/logo-sync.yml` automation workflow
- `data/companies.csv` fallback source list
- `output/` generated sync artifacts

## GitHub secrets required

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`

Optional:

- `R2_PUBLIC_BASE_URL` (for public URL in manifest, e.g. `https://media.example.com`)
- `ATRIVEO_API_URL` (if sourcing companies from Atriveo API)
- `ATRIVEO_API_TOKEN` (bearer token for Atriveo API)
- `SYNC_ENABLED` (`true`/`false`, hard kill switch for workflow; default behavior is enabled)
- `SAFE_MAX_STORAGE_BYTES` (default `9000000000`, stops new uploads when projected manifest storage exceeds this)
- `SAFE_MAX_CLASS_A_MONTH` (default `900000`, stops new uploads when monthly write counter exceeds this)

## Local usage

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 1) Sync from CSV

```bash
python scripts/sync_logos.py --source csv --source-file data/companies.csv
```

### 2) Sync from Atriveo API

```bash
export ATRIVEO_API_URL="https://your-api.example.com"
export ATRIVEO_API_TOKEN="..."
python scripts/sync_logos.py --source atriveo
```

### 3) Dry run (no upload)

```bash
python scripts/sync_logos.py --source auto --dry-run
```

## Notes

- Default resync window is 30 days (skip recent entries unless `--full-sync`).
- The script exits non-zero when failures occur; workflow still uploads artifacts.
- Usage ledger is stored in `output/usage-ledger.json` and committed by workflow.
- Guardrails stop *new sync uploads* before limits, but Cloudflare billing itself does not provide a strict global hard-stop for all R2 reads/writes.
