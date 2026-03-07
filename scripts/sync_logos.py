#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import httpx

try:
    from logohunter import LogoHunter
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Failed to import logohunter. Install dependencies with `pip install -r requirements.txt`."
    ) from exc


@dataclass(frozen=True)
class CompanyTarget:
    company: str
    domain: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def normalize_company(raw: str) -> str:
    return re.sub(r"\s+", " ", str(raw or "").strip())


def normalize_domain(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if not value:
        return ""
    if "://" in value:
        parsed = urlparse(value)
        value = parsed.netloc or parsed.path
    value = value.split("@")[-1]
    value = value.split(":")[0]
    if value.startswith("www."):
        value = value[4:]
    return value.strip().strip("/")


def safe_slug(raw: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(raw or "").strip().lower()).strip("-")
    return clean or "unknown"


def detect_content_type_and_ext(payload: bytes) -> Tuple[str, str]:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", "png"
    if payload.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", "jpg"
    if payload.startswith(b"GIF87a") or payload.startswith(b"GIF89a"):
        return "image/gif", "gif"
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image/webp", "webp"

    head = payload[:512].lstrip().lower()
    if head.startswith(b"<?xml") or head.startswith(b"<svg"):
        return "image/svg+xml", "svg"

    return "application/octet-stream", "bin"


def read_existing_manifest(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        company = normalize_company(str(item.get("company", "")))
        domain = normalize_domain(str(item.get("domain", "")))
        if not company or not domain:
            continue
        out[f"{company}|{domain}"] = item
    return out


def parse_iso8601(raw: str) -> Optional[datetime]:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def should_resync(existing: Dict[str, Any], resync_days: int, full_sync: bool) -> bool:
    if full_sync:
        return True
    synced_at = parse_iso8601(str(existing.get("synced_at", "")))
    if synced_at is None:
        return True
    return synced_at <= utc_now() - timedelta(days=max(0, resync_days))


def load_targets_from_csv(path: Path) -> List[CompanyTarget]:
    if not path.exists():
        raise FileNotFoundError(f"CSV source file not found: {path}")

    targets: Dict[str, CompanyTarget] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = normalize_company(row.get("company", ""))
            domain = normalize_domain(row.get("domain", "") or row.get("website", ""))
            if not company or not domain:
                continue
            key = f"{company}|{domain}"
            targets[key] = CompanyTarget(company=company, domain=domain)
    return sorted(targets.values(), key=lambda t: (t.company.lower(), t.domain))


def domain_from_url(raw_url: str) -> str:
    raw_url = str(raw_url or "").strip()
    if not raw_url:
        return ""
    try:
        parsed = urlparse(raw_url)
    except Exception:
        return ""
    return normalize_domain(parsed.netloc)


async def load_targets_from_atriveo(api_url: str, token: str, limit: int = 100) -> List[CompanyTarget]:
    base = str(api_url or "").rstrip("/")
    if not base:
        raise ValueError("ATRIVEO_API_URL is required for atriveo source mode.")
    if not token:
        raise ValueError("ATRIVEO_API_TOKEN is required for atriveo source mode.")

    headers = {"Authorization": f"Bearer {token}"}
    page = 1
    max_per_page = min(max(limit, 1), 100)
    total = 1
    rows_seen = 0

    company_to_domain_counts: Dict[str, Dict[str, int]] = {}

    async with httpx.AsyncClient(timeout=25.0, headers=headers) as client:
        while rows_seen < total:
            url = f"{base}/api/jobs?page={page}&limit={max_per_page}&status=all"
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data") or []
            total = int(payload.get("total") or 0)
            if not isinstance(data, list) or not data:
                break

            for row in data:
                if not isinstance(row, dict):
                    continue
                company = normalize_company(row.get("company", ""))
                if not company:
                    continue

                domain = domain_from_url(str(row.get("job_link", "") or ""))
                if not domain:
                    continue

                bucket = company_to_domain_counts.setdefault(company, {})
                bucket[domain] = bucket.get(domain, 0) + 1

            rows_seen += len(data)
            if len(data) < max_per_page:
                break
            page += 1

    out: List[CompanyTarget] = []
    for company, counts in company_to_domain_counts.items():
        best_domain = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        out.append(CompanyTarget(company=company, domain=best_domain))
    return sorted(out, key=lambda t: (t.company.lower(), t.domain))


def build_object_key(company: str, domain: str, sha256_hex: str, ext: str) -> str:
    return f"logos/{safe_slug(company)}/{safe_slug(domain)}/{sha256_hex[:16]}.{ext}"


def create_r2_client() -> Tuple[Any, str, Optional[str]]:
    account_id = str(os.getenv("R2_ACCOUNT_ID", "")).strip()
    access_key = str(os.getenv("R2_ACCESS_KEY_ID", "")).strip()
    secret_key = str(os.getenv("R2_SECRET_ACCESS_KEY", "")).strip()
    bucket = str(os.getenv("R2_BUCKET", "")).strip()
    public_base = str(os.getenv("R2_PUBLIC_BASE_URL", "")).strip() or None

    missing = []
    if not account_id:
        missing.append("R2_ACCOUNT_ID")
    if not access_key:
        missing.append("R2_ACCESS_KEY_ID")
    if not secret_key:
        missing.append("R2_SECRET_ACCESS_KEY")
    if not bucket:
        missing.append("R2_BUCKET")
    if missing:
        raise RuntimeError(f"Missing required R2 env vars: {', '.join(missing)}")

    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )
    return client, bucket, public_base


def build_public_url(public_base: Optional[str], object_key: str) -> Optional[str]:
    if not public_base:
        return None
    base = public_base.rstrip("/")
    key = "/".join([part for part in object_key.split("/") if part])
    return f"{base}/{key}"


def ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


async def fetch_logo_bytes(hunter: LogoHunter, domain: str) -> bytes:
    # Prefer PNG output for stable browser usage.
    data = await hunter.get_customer_logo(domain, output_format="PNG", resize_to=(512, 512))
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    if data is None:
        return b""
    return bytes(data)


def parse_positive_int_env(name: str, default_value: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return int(default_value)
    try:
        parsed = int(raw)
    except Exception:
        return int(default_value)
    return parsed if parsed > 0 else int(default_value)


def month_key_utc() -> str:
    return utc_now().strftime("%Y-%m")


def read_usage_ledger(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "months": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "months": {}}
    if not isinstance(data, dict):
        return {"version": 1, "months": {}}
    months = data.get("months")
    if not isinstance(months, dict):
        months = {}
    return {
        "version": int(data.get("version") or 1),
        "months": months,
    }


def get_month_class_a_writes(ledger: Dict[str, Any], month: str) -> int:
    months = ledger.get("months")
    if not isinstance(months, dict):
        return 0
    row = months.get(month)
    if not isinstance(row, dict):
        return 0
    try:
        value = int(row.get("class_a_writes", 0))
    except Exception:
        value = 0
    return max(0, value)


def set_month_class_a_writes(ledger: Dict[str, Any], month: str, value: int) -> None:
    months = ledger.setdefault("months", {})
    if not isinstance(months, dict):
        ledger["months"] = {}
        months = ledger["months"]
    current = months.get(month)
    if not isinstance(current, dict):
        current = {}
    current["class_a_writes"] = int(max(0, value))
    current["updated_at"] = iso_now()
    months[month] = current


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync company logos to Cloudflare R2.")
    parser.add_argument("--source", choices=["auto", "csv", "atriveo"], default="auto")
    parser.add_argument("--source-file", default="data/companies.csv")
    parser.add_argument("--output", default="output/logos.json")
    parser.add_argument("--report", default="output/last-run.json")
    parser.add_argument("--ledger", default="output/usage-ledger.json")
    parser.add_argument("--resync-days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=0, help="Limit number of companies processed (0 = all).")
    parser.add_argument("--full-sync", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


async def run(args: argparse.Namespace) -> int:
    started_at = iso_now()
    output_path = Path(args.output)
    report_path = Path(args.report)
    ledger_path = Path(args.ledger)
    source_file = Path(args.source_file)

    source_mode = args.source
    if source_mode == "auto":
        source_mode = "atriveo" if os.getenv("ATRIVEO_API_URL") and os.getenv("ATRIVEO_API_TOKEN") else "csv"

    if source_mode == "atriveo":
        targets = await load_targets_from_atriveo(
            api_url=os.getenv("ATRIVEO_API_URL", ""),
            token=os.getenv("ATRIVEO_API_TOKEN", ""),
            limit=100,
        )
    else:
        targets = load_targets_from_csv(source_file)

    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    if not targets:
        print("No company targets found. Nothing to sync.")
        return 0

    existing_map = read_existing_manifest(output_path)
    usage_ledger = read_usage_ledger(ledger_path)
    active_month = month_key_utc()
    monthly_class_a_writes_used = get_month_class_a_writes(usage_ledger, active_month)
    max_storage_bytes = parse_positive_int_env("SAFE_MAX_STORAGE_BYTES", 9_000_000_000)
    max_class_a_month = parse_positive_int_env("SAFE_MAX_CLASS_A_MONTH", 900_000)

    current_storage_bytes = 0
    for item in existing_map.values():
        try:
            current_storage_bytes += int(item.get("byte_size") or 0)
        except Exception:
            continue

    if args.dry_run:
        r2_client = None
        bucket_name = ""
        public_base = None
    else:
        r2_client, bucket_name, public_base = create_r2_client()

    hunter = LogoHunter()

    updated_entries: Dict[str, Dict[str, Any]] = dict(existing_map)
    failures: List[Dict[str, Any]] = []

    processed = 0
    skipped_recent = 0
    fetched = 0
    uploaded = 0
    unchanged = 0
    budget_skipped = 0
    storage_budget_hit = False
    class_a_budget_hit = False

    for target in targets:
        key = f"{target.company}|{target.domain}"
        existing = existing_map.get(key, {})
        if existing and not should_resync(existing, args.resync_days, args.full_sync):
            skipped_recent += 1
            continue

        processed += 1

        try:
            payload = await fetch_logo_bytes(hunter, target.domain)
            if not payload:
                raise RuntimeError("LogoHunter returned empty payload.")
        except Exception as exc:
            failures.append(
                {
                    "company": target.company,
                    "domain": target.domain,
                    "error": str(exc),
                }
            )
            continue

        fetched += 1
        sha256_hex = hashlib.sha256(payload).hexdigest()
        content_type, ext = detect_content_type_and_ext(payload)
        object_key = build_object_key(target.company, target.domain, sha256_hex, ext)
        try:
            existing_byte_size = int(existing.get("byte_size") or 0)
        except Exception:
            existing_byte_size = 0

        if existing and str(existing.get("sha256_hex", "")) == sha256_hex:
            unchanged += 1
            object_key = str(existing.get("object_key") or object_key)
        elif not args.dry_run:
            projected_class_a = monthly_class_a_writes_used + 1
            projected_storage = current_storage_bytes - max(0, existing_byte_size) + len(payload)

            if projected_class_a > max_class_a_month:
                class_a_budget_hit = True
                budget_skipped += 1
                continue

            if projected_storage > max_storage_bytes:
                storage_budget_hit = True
                budget_skipped += 1
                continue

            assert r2_client is not None
            r2_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=payload,
                ContentType=content_type,
                CacheControl="public, max-age=31536000, immutable",
                Metadata={
                    "company": target.company,
                    "domain": target.domain,
                    "sha256": sha256_hex,
                    "source": "logohunter",
                },
            )
            uploaded += 1
            monthly_class_a_writes_used = projected_class_a
            current_storage_bytes = projected_storage

        updated_entries[key] = {
            "company": target.company,
            "domain": target.domain,
            "object_key": object_key,
            "content_type": content_type,
            "sha256_hex": sha256_hex,
            "byte_size": len(payload),
            "source": "logohunter",
            "synced_at": iso_now(),
            "public_url": build_public_url(public_base, object_key),
        }

    manifest = sorted(
        updated_entries.values(),
        key=lambda item: (str(item.get("company", "")).lower(), str(item.get("domain", "")).lower()),
    )

    final_storage_bytes = 0
    for item in manifest:
        try:
            final_storage_bytes += int(item.get("byte_size") or 0)
        except Exception:
            continue

    if not args.dry_run:
        set_month_class_a_writes(usage_ledger, active_month, monthly_class_a_writes_used)

    report = {
        "started_at": started_at,
        "finished_at": iso_now(),
        "source": source_mode,
        "dry_run": bool(args.dry_run),
        "full_sync": bool(args.full_sync),
        "resync_days": int(args.resync_days),
        "targets_total": len(targets),
        "processed": processed,
        "skipped_recent": skipped_recent,
        "fetched": fetched,
        "uploaded": uploaded,
        "unchanged": unchanged,
        "budget_skipped": budget_skipped,
        "budget_guard": {
            "max_storage_bytes": max_storage_bytes,
            "max_class_a_month": max_class_a_month,
            "active_month": active_month,
            "month_class_a_writes_used": monthly_class_a_writes_used,
            "estimated_storage_bytes": final_storage_bytes,
            "storage_budget_hit": storage_budget_hit,
            "class_a_budget_hit": class_a_budget_hit,
        },
        "failed": len(failures),
        "failures": failures,
    }

    ensure_dir(output_path)
    ensure_dir(report_path)
    ensure_dir(ledger_path)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    ledger_path.write_text(json.dumps(usage_ledger, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2))
    return 0 if not failures else 2


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except Exception as exc:
        print(f"Logo sync failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
