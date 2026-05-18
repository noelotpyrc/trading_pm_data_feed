#!/usr/bin/env python3
"""
Fetch historical PM trades per 15-min btc-updown epoch slug.

Backup data source for the live order-book polling pipeline. Walks the
trades endpoint newest->oldest per slug, early-stops once a trade
timestamp drops below `cutoff_ts` (default: 60 s before epoch start).

The Polymarket /trades endpoint has a hard ~4000-row cap on (limit*offset)
regardless of `limit`. Busy epochs in Jan-Mar 2026 routinely exceed this.
When pagination terminates without reaching `cutoff_ts` we mark the slug
as `capped` in the run summary so partial coverage is visible.

Per-slug parquet checkpoint: re-runs skip slugs whose output already
exists (unless --force).

Default output: /Volumes/Extreme SSD/vps_madrid_backup/pm_btc15updown_trades/
Override with --out-dir.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

GAMMA_URL = "https://gamma-api.polymarket.com/events/keyset?slug={slug}"
TRADES_URL = "https://data-api.polymarket.com/trades"
SLUG_PREFIX = "btc-updown-15m-"
EPOCH_S = 15 * 60

DEFAULT_OUT = Path("/Volumes/Extreme SSD/vps_madrid_backup/pm_btc15updown_trades")
KEEP_FIELDS = ["timestamp", "asset", "outcome", "side", "price", "size"]
API_HARD_CAP = 4000  # /trades returns at most this many rows total per market


def _http_get(url: str, timeout: float = 15, retries: int = 2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pm_btc15updown_data/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(5 + 5 * attempt)
                last_err = e
                continue
            return e.code, e.read()[:300]
        except urllib.error.URLError as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 + attempt)
    return None, str(last_err)


def slug_for_epoch(epoch_ts: int) -> str:
    return f"{SLUG_PREFIX}{epoch_ts}"


def resolve_condition_id(slug: str, sleep_sec: float) -> str | None:
    time.sleep(sleep_sec)
    s, p = _http_get(GAMMA_URL.format(slug=slug))
    if s != 200 or not isinstance(p, dict):
        return None
    events = p.get("events", []) or []
    if not events: return None
    markets = events[0].get("markets", []) or []
    if not markets: return None
    return markets[0].get("conditionId")


def fetch_trades(condition_id: str, cutoff_ts: int, limit: int, sleep_sec: float):
    """Paginate newest->oldest. Stop when any trade crosses cutoff_ts.
    Returns (kept_trades, n_pages, reached_cutoff).
    reached_cutoff=False means pagination ended before we saw cutoff, i.e.
    the API hard-cap was hit (or the market truly has no older trades)."""
    kept = []
    offset = 0
    n_pages = 0
    reached_cutoff = False
    while True:
        time.sleep(sleep_sec)
        url = f"{TRADES_URL}?market={condition_id}&limit={limit}&offset={offset}"
        s, page = _http_get(url)
        n_pages += 1
        if s != 200 or not isinstance(page, list):
            print(f"    warn: bad response status={s} at offset={offset}")
            break
        if not page:
            break
        kept.extend(t for t in page if t["timestamp"] >= cutoff_ts)
        if page[-1]["timestamp"] < cutoff_ts:
            reached_cutoff = True
            break
        if len(page) < limit:
            break
        offset += limit
    return kept, n_pages, reached_cutoff


def slugs_for_date_range(start: date, end: date) -> list[str]:
    out = []
    d = start
    while d <= end:
        midnight = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        midnight_ts = int(midnight.timestamp())
        for i in range(96):
            out.append(slug_for_epoch(midnight_ts + i * EPOCH_S))
        d += timedelta(days=1)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", type=date.fromisoformat, required=True,
                   help="UTC date, inclusive (YYYY-MM-DD)")
    p.add_argument("--end",   type=date.fromisoformat, required=True,
                   help="UTC date, inclusive (YYYY-MM-DD)")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT,
                   help=f"output directory (default: {DEFAULT_OUT})")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--sleep-sec", type=float, default=0.0,
                   help="sleep between API calls (PM /trades cap is "
                        "~120 req/min, ~0.9s RTT alone already uses ~56%%)")
    p.add_argument("--cutoff-pre-min", type=int, default=1,
                   help="stop pagination once trades are >N min before epoch_start")
    p.add_argument("--force", action="store_true",
                   help="re-fetch even if per-slug parquet exists")
    args = p.parse_args()

    # Hard fail if out-dir parent does not exist (e.g. SSD unmounted).
    if not args.out_dir.parent.exists():
        raise SystemExit(f"error: parent of --out-dir does not exist: {args.out_dir.parent}\n"
                         f"(if using the default external SSD path, plug it in)")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    slugs = slugs_for_date_range(args.start, args.end)
    print(f"Slugs in [{args.start} .. {args.end}]: {len(slugs)}")
    print(f"  output dir: {args.out_dir}")
    print(f"  limit={args.limit}  sleep_sec={args.sleep_sec}  cutoff_pre_min={args.cutoff_pre_min}")
    print()

    summary_rows = []
    t0 = time.perf_counter()
    for i, slug in enumerate(slugs, 1):
        out_path = args.out_dir / f"{slug}.jsonl"
        if out_path.exists() and not args.force:
            summary_rows.append({"slug": slug, "status": "skip-exists",
                                 "n_trades": -1, "n_pages": 0, "capped": False})
            continue

        epoch_ts = int(slug.rsplit("-", 1)[1])
        cutoff_ts = epoch_ts - args.cutoff_pre_min * 60

        cid = resolve_condition_id(slug, args.sleep_sec)
        if cid is None:
            print(f"  [{i:>4d}/{len(slugs)}] {slug}: NO MARKET (skip)")
            summary_rows.append({"slug": slug, "status": "no-market",
                                 "n_trades": 0, "n_pages": 0, "capped": False})
            continue

        trades, n_pages, reached_cutoff = fetch_trades(
            cid, cutoff_ts, args.limit, args.sleep_sec)
        capped = (not reached_cutoff) and len(trades) >= API_HARD_CAP - args.limit

        # Sort ascending by timestamp, lean fields, atomic write via .tmp rename.
        trades_sorted = sorted(trades, key=lambda t: t["timestamp"])
        tmp_path = out_path.with_suffix(".jsonl.tmp")
        with tmp_path.open("w") as f:
            for t in trades_sorted:
                row = {k: t.get(k) for k in KEEP_FIELDS}
                f.write(json.dumps(row) + "\n")
        tmp_path.replace(out_path)
        n = len(trades_sorted)
        min_ts = int(trades_sorted[0]["timestamp"]) if trades_sorted else None
        max_ts = int(trades_sorted[-1]["timestamp"]) if trades_sorted else None

        elapsed = time.perf_counter() - t0
        flag = " [CAPPED]" if capped else ""
        print(f"  [{i:>4d}/{len(slugs)}] {slug}: {n:>5d} trades, "
              f"{n_pages} page(s){flag}  ({elapsed/i:.2f}s/slug avg, {elapsed:.1f}s total)")
        summary_rows.append({
            "slug": slug,
            "status": "ok",
            "n_trades": n,
            "n_pages": n_pages,
            "capped": capped,
            "earliest_ts": min_ts,
            "latest_ts": max_ts,
        })

    elapsed = time.perf_counter() - t0
    summary_path = args.out_dir / f"_run_summary_{args.start}_{args.end}.json"
    summary_path.write_text(json.dumps({
        "start": args.start.isoformat(),
        "end":   args.end.isoformat(),
        "n_slugs":  len(slugs),
        "elapsed_seconds": round(elapsed, 1),
        "totals": {
            "ok":        sum(1 for r in summary_rows if r["status"] == "ok"),
            "skip":      sum(1 for r in summary_rows if r["status"] == "skip-exists"),
            "no_market": sum(1 for r in summary_rows if r["status"] == "no-market"),
            "capped":    sum(1 for r in summary_rows if r.get("capped")),
            "trades_total": sum(r["n_trades"] for r in summary_rows if r["n_trades"] > 0),
        },
        "per_slug": summary_rows,
    }, indent=2))
    print()
    print(f"Done. {elapsed:.1f}s total ({elapsed/60:.1f} min) "
          f"for {len(slugs)} slugs.")
    print(f"  capped:  {sum(1 for r in summary_rows if r.get('capped'))}")
    print(f"  summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
