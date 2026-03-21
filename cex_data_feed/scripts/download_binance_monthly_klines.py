#!/usr/bin/env python3
"""
Simple downloader for Binance monthly klines ZIPs (Spot or USDT-M Futures).

- Constructs monthly URLs by pattern without scraping
- Verifies existence with a HEAD request (falls back to GET if needed)
- Downloads files into a target directory
- Skips files that already exist and match the remote Content-Length

Example file patterns:
  Futures: https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2020-01.zip
  Spot:    https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2020-01.zip

Usage examples:
  # Futures (default)
  python -m cex_data_feed.scripts.download_binance_monthly_klines \
    --out "/path/to/ohlvc" --start 2020-01 --symbol BTCUSDT --interval 1h

  # Spot
  python -m cex_data_feed.scripts.download_binance_monthly_klines \
    --market spot --out "/path/to/spot_klines" --start 2020-01 --symbol BTCUSDT --interval 1h

You can adjust symbol, interval, market, and date range via CLI flags.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


BASE_URL_TEMPLATE = "https://data.binance.vision/data/{market_path}/monthly/klines"
MARKET_PATHS = {
    "futures": "futures/um",
    "spot": "spot",
}
DEFAULT_OUTPUT_DIR = "/tmp/klines_1m"


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _http_request(url: str, *, method: str = "GET", timeout: float = 60.0):
    req = Request(url, method=method, headers={"User-Agent": USER_AGENT})
    return urlopen(req, timeout=timeout)


def _month_iter(start_yyyymm: str, end_yyyymm: Optional[str] = None) -> List[str]:
    """Generate inclusive list of months in YYYY-MM between start and end.

    If end is None, uses current UTC month.
    """
    start_dt = datetime.strptime(start_yyyymm, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
    if end_yyyymm is None:
        now = datetime.now(timezone.utc)
        end_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end_dt = datetime.strptime(end_yyyymm, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
    if end_dt < start_dt:
        return []

    months: List[str] = []
    y, m = start_dt.year, start_dt.month
    while (y < end_dt.year) or (y == end_dt.year and m <= end_dt.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def build_monthly_url(symbol: str, interval: str, yyyymm: str, market: str = "futures") -> str:
    market_path = MARKET_PATHS.get(market, MARKET_PATHS["futures"])
    base_url = BASE_URL_TEMPLATE.format(market_path=market_path)
    return (
        f"{base_url}/{symbol}/{interval}/"
        f"{symbol}-{interval}-{yyyymm}.zip"
    )


def url_exists(url: str, timeout: float = 30.0) -> Tuple[bool, Optional[int]]:
    """Check if URL exists using HEAD, returning (exists, content_length).

    Falls back to a tiny GET if HEAD fails (some servers may not support HEAD).
    """
    try:
        with _http_request(url, method="HEAD", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            size = int(length) if length is not None else None
            return True, size
    except HTTPError as e:
        # 404 -> not found; others may still be retried with GET
        if getattr(e, "code", None) == 404:
            return False, None
        # Try GET fallback
    except URLError:
        pass

    # Fallback: small GET attempt
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
    except HTTPError as e:
        # Some servers may not support HEAD; ignore
        print(f"[INFO] HEAD failed for {url}: {e}. Will GET instead.")
    except URLError as e:
        print(f"[WARN] HEAD connection error for {url}: {e}")
    except Exception as e:
        print(f"[WARN] HEAD unexpected error for {url}: {e}")
    return None


def _stream_download(url: str, dest_path: str, timeout: float = 120.0) -> None:
    tmp_path = dest_path + ".part"
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    bytes_written = 0
    start_time = time.time()
    try:
        with _http_request(url, method="GET", timeout=timeout) as resp:
            with open(tmp_path, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 512)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_written += len(chunk)
                    # Lightweight progress indicator
                    if bytes_written % (1024 * 1024 * 50) == 0:
                        elapsed = time.time() - start_time
                        mb = bytes_written / (1024 * 1024)
                        print(
                            f"  downloaded ~{mb:.1f} MiB in {elapsed:.1f}s"
                        )
        os.replace(tmp_path, dest_path)
    finally:
        # Clean up partial files on error/interruption
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def download_if_needed(url: str, output_dir: str, timeout: float = 120.0) -> Tuple[str, str]:
    """Download URL into output_dir if missing or size mismatch.

    Returns a tuple of (status, path):
      - status in {"skipped", "downloaded", "failed"}
      - path is the local file path
    """
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
        else:
            print(
                f"[INFO] Re-downloading due to size mismatch or unknown size: {file_name}"
            )
    try:
        _stream_download(url, dest_path, timeout=timeout)
        # Verify size when possible
        if remote_size is not None:
            try:
                if os.path.getsize(dest_path) != remote_size:
                    print(
                        f"[WARN] Size mismatch after download for {file_name}"
                    )
            except OSError:
                pass
        return ("downloaded", dest_path)
    except (HTTPError, URLError) as e:
        print(f"[ERROR] Download failed for {url}: {e}")
        return ("failed", dest_path)
    except Exception as e:
        print(f"[ERROR] Unexpected error for {url}: {e}")
        traceback.print_exc()
        return ("failed", dest_path)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Download Binance monthly kline ZIPs (Spot or Futures).")
    )
    parser.add_argument(
        "--market",
        choices=["spot", "futures"],
        default="futures",
        help="Market type: 'spot' or 'futures' (default: futures)",
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    parser.add_argument("--interval", default="1m", help="Interval, e.g., 1m")
    parser.add_argument("--start", default="2020-01", help="Start month YYYY-MM (inclusive)")
    parser.add_argument("--end", default=None, help="End month YYYY-MM (inclusive); default: current month")
    parser.add_argument(
        "--out",
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Output directory for downloaded ZIPs. Will be created if missing."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list files to be downloaded; do not download.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Network timeout per request in seconds (default: 120).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    months = _month_iter(args.start, args.end)
    if not months:
        print("[ERROR] Empty month range. Check --start/--end.")
        return 2

    # Build URLs and filter to those that exist
    candidates = [build_monthly_url(args.symbol, args.interval, m, args.market) for m in months]
    existing: List[Tuple[str, Optional[int]]] = []
    print("[INFO] Probing monthly files...")
    for url in candidates:
        ok, size = url_exists(url, timeout=args.timeout)
        if ok:
            existing.append((url, size))

    if not existing:
        print("[ERROR] No files found for the specified range.")
        return 2

    os.makedirs(args.out, exist_ok=True)

    if args.dry_run:
        for url, size in existing:
            if size is not None:
                print(f"{url}  size={size}")
            else:
                print(url)
        print(f"[INFO] {len(existing)} files available in range {months[0]}..{months[-1]}")
        return 0

    num_downloaded = 0
    num_skipped = 0
    num_failed = 0
    for idx, (url, _) in enumerate(existing, start=1):
        file_name = os.path.basename(urlparse(url).path)
        print(f"[{idx}/{len(existing)}] {file_name}")
        status, _ = download_if_needed(url, args.out, timeout=args.timeout)
        if status == "downloaded":
            num_downloaded += 1
        elif status == "skipped":
            num_skipped += 1
        else:
            num_failed += 1

    print(
        "[INFO] Done. "
        f"downloaded={num_downloaded}, skipped={num_skipped}, failed={num_failed}"
    )
    return 0 if num_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
