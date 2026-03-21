#!/usr/bin/env python3
"""
Downloader for Binance daily kline ZIPs (Spot or USDT-M Futures).

Use this to backfill partial months (e.g., current month) that aren't yet
available in the monthly dataset. Files are under:
  Futures: https://data.binance.vision/data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/
  Spot:    https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/

Example:
  # Futures (default)
  python -m cex_data_feed.scripts.download_binance_daily_klines \
    --symbol BTCUSDT --interval 1h \
    --start-date 2025-09-01 --end-date 2025-09-14 \
    --out "/path/to/daily_1h"

  # Spot
  python -m cex_data_feed.scripts.download_binance_daily_klines \
    --market spot --symbol BTCUSDT --interval 1h \
    --start-date 2025-09-01 --end-date 2025-09-14 \
    --out "/path/to/spot_daily_1h"

Then merge with merge_binance_klines.py by pointing --in-dir to the folder
containing the daily ZIPs.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


BASE_URL_TEMPLATE = "https://data.binance.vision/data/{market_path}/daily/klines"
MARKET_PATHS = {
    "futures": "futures/um",
    "spot": "spot",
}

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _http_request(url: str, *, method: str = "GET", timeout: float = 60.0):
    req = Request(url, method=method, headers={"User-Agent": USER_AGENT})
    return urlopen(req, timeout=timeout)


def _date_iter(start_date: str, end_date: Optional[str] = None) -> List[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_date is None:
        end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end < start:
        return []
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def build_daily_url(symbol: str, interval: str, yyyymmdd: str, market: str = "futures") -> str:
    market_path = MARKET_PATHS.get(market, MARKET_PATHS["futures"])
    base_url = BASE_URL_TEMPLATE.format(market_path=market_path)
    return f"{base_url}/{symbol}/{interval}/{symbol}-{interval}-{yyyymmdd}.zip"


def url_exists(url: str, timeout: float = 30.0) -> Tuple[bool, Optional[int]]:
    try:
        with _http_request(url, method="HEAD", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            size = int(length) if length is not None else None
            return True, size
    except HTTPError as e:
        if getattr(e, "code", None) == 404:
            return False, None
    except URLError:
        pass
    try:
        with _http_request(url, method="GET", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            size = int(length) if length is not None else None
            return True, size
    except Exception:
        return False, None


def _get_remote_content_length(url: str, timeout: float = 60.0) -> Optional[int]:
    try:
        with _http_request(url, method="HEAD", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            if length is not None:
                return int(length)
    except Exception:
        return None
    return None


def _stream_download(url: str, dest_path: str, timeout: float = 120.0) -> None:
    tmp_path = dest_path + ".part"
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with _http_request(url, method="GET", timeout=timeout) as resp:
        with open(tmp_path, "wb") as f:
            while True:
                chunk = resp.read(1024 * 512)
                if not chunk:
                    break
                f.write(chunk)
    os.replace(tmp_path, dest_path)
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception:
        pass


def download_if_needed(url: str, output_dir: str, timeout: float = 120.0) -> Tuple[str, str]:
    file_name = os.path.basename(urlparse(url).path)
    if not file_name:
        return ("failed", "")
    dest_path = os.path.join(output_dir, file_name)

    remote_size = _get_remote_content_length(url, timeout=timeout)
    if os.path.exists(dest_path):
        try:
            local_size = os.path.getsize(dest_path)
        except OSError:
            local_size = None
        if remote_size is not None and local_size == remote_size:
            return ("skipped", dest_path)
    try:
        _stream_download(url, dest_path, timeout=timeout)
        return ("downloaded", dest_path)
    except (HTTPError, URLError):
        return ("failed", dest_path)
    except Exception:
        return ("failed", dest_path)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=("Download Binance daily kline ZIPs (Spot or Futures)."))
    parser.add_argument(
        "--market",
        choices=["spot", "futures"],
        default="futures",
        help="Market type: 'spot' or 'futures' (default: futures)",
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    parser.add_argument("--interval", default="1h", help="Interval, e.g., 1h")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD (inclusive); default: today UTC")
    parser.add_argument("--out", required=True, help="Output directory for daily ZIPs")
    parser.add_argument("--dry-run", action="store_true", help="Only list URLs; do not download")
    parser.add_argument("--timeout", type=float, default=120.0, help="Network timeout per request in seconds")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    days = _date_iter(args.start_date, args.end_date)
    if not days:
        print("[ERROR] Empty date range. Check --start-date/--end-date.")
        return 2

    urls = [build_daily_url(args.symbol, args.interval, d, args.market) for d in days]
    available = []
    for u in urls:
        ok, size = url_exists(u, timeout=args.timeout)
        if ok:
            available.append((u, size))
    if not available:
        print("[ERROR] No daily files found for the specified range.")
        return 2

    os.makedirs(args.out, exist_ok=True)
    if args.dry_run:
        for u, size in available:
            print(u if size is None else f"{u}  size={size}")
        print(f"[INFO] {len(available)} daily files available in {days[0]}..{days[-1]}")
        return 0

    num_downloaded = 0
    num_skipped = 0
    num_failed = 0
    for idx, (url, _) in enumerate(available, start=1):
        print(f"[{idx}/{len(available)}] {os.path.basename(urlparse(url).path)}")
        status, _ = download_if_needed(url, args.out, timeout=args.timeout)
        if status == "downloaded":
            num_downloaded += 1
        elif status == "skipped":
            num_skipped += 1
        else:
            num_failed += 1
    print(f"[INFO] Done. downloaded={num_downloaded}, skipped={num_skipped}, failed={num_failed}")
    return 0 if num_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
