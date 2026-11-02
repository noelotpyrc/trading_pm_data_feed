"""CLI: validate frozen rules locally, or explicitly launch the recorder."""
import argparse
import json
import logging
import signal
from pathlib import Path

from .service import Service
from .storage import open_session, process_lock, validate_rules


def main(argv=None):
    parser = argparse.ArgumentParser(description="NBA receive-clock recorder; no orders or live signals")
    parser.add_argument("--rules", type=Path, required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--data-dir", type=Path, default=Path("data/pm_nba_dryrun"))
    parser.add_argument("--backup-to", type=Path, help="Existing mounted SSD directory; snapshot on startup, then weekly")
    parser.add_argument("--check", action="store_true", help="Validate rules without creating a session or using the network")
    args = parser.parse_args(argv)
    try:
        validate_rules(json.loads(args.rules.read_bytes()))
    except (OSError, ValueError, TypeError) as exc:
        parser.error(str(exc))
    if args.check:
        print("NBA rules valid")
        return
    if not args.run_id:
        parser.error("--run-id is required to record")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with process_lock(args.data_dir, "nba"):
        store, metadata = open_session(args.data_dir, args.run_id, args.rules)
        service = Service(store, backup_to=args.backup_to)
        def stop(_signum, _frame):
            service.stop.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, stop)
        logging.info("NBA recording session %s", metadata)
        service.run()


if __name__ == "__main__":
    main()
