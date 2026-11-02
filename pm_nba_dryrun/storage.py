"""Immutable rules, fresh process sessions, and the existing JSONL writer."""
from __future__ import annotations

import hashlib
import json
import re
import socket
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pm_soccer_dryrun.storage import RecordingError, Store as SoccerStore, day, now_ms, process_lock


class Store(SoccerStore):
    """Reuse JSONL durability without soccer-specific engine health counters."""

    def summary(self):
        with self.lock:
            return dict(self._counts)


def validate_rules(rules):
    if not isinstance(rules, dict):
        raise ValueError("rules must be an object")
    if rules.get("schema_version") != 1:
        raise ValueError("rules schema_version must be 1")
    # Entry frequency and the decision rule are analysis choices, frozen in pm_lob_nba before the first offline look.
    if "entries_per_game" in rules or "decision_rule" in rules:
        raise ValueError("entries_per_game / decision_rule belong in the analysis rules (pm_lob_nba), not the recorder")
    if not isinstance(rules.get("cells"), list) or len(rules["cells"]) != 4:
        raise ValueError("rules must contain the four frozen report 07 cells")
    for cell in rules["cells"]:
        if not isinstance(cell, dict) or not all(k in cell for k in ("clock_bin", "price_bins")):
            raise ValueError("Incomplete cell definition")
        bins = cell["price_bins"]
        if not isinstance(bins, list) or len(bins) != 2 or not all(type(b) is int for b in bins) or not 0 <= bins[0] <= bins[1] <= 19:
            raise ValueError("price_bins must be inclusive indices within 0..19")
    for key in ("observations", "clock_source", "clock_bins", "clock_boundaries", "price_bin_width", "price_bins", "entry", "size", "gap_policy"):
        if not rules.get(key):
            raise ValueError(f"Missing frozen rule: {key}")
    entry = rules["entry"]
    if not isinstance(entry, dict) or not all(k in entry for k in ("delay_ms", "price", "fee", "hold")):
        raise ValueError("Incomplete entry definition")
    # JSON rejects nonfinite numbers throughout, including user extensions.
    json.dumps(rules, allow_nan=False)


def open_session(root, run_id, rules_path):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id) or run_id in {".", ".."}:
        raise ValueError("run-id must be a plain directory name")
    raw = Path(rules_path).read_bytes()
    rules = json.loads(raw)
    validate_rules(rules)
    digest = hashlib.sha256(raw).hexdigest()
    base = Path(root).resolve()
    run = base / run_id
    if not run.resolve().is_relative_to(base):
        raise ValueError("Run directory escapes data root")
    run.mkdir(parents=True, exist_ok=True)
    frozen = run / "rules.json"
    if frozen.exists():
        if frozen.read_bytes() != raw:
            raise ValueError("Rules changed: choose a new run-id; runs must never be pooled")
        manifest = json.loads((run / "run.json").read_bytes())
        if manifest.get("rules_sha256") != digest or manifest.get("run_id") != run_id:
            raise ValueError("Frozen rules do not match run.json; choose a new run-id")
    else:
        if any(run.iterdir()):
            raise ValueError("Existing run has no frozen rules")
        with frozen.open("xb") as handle:
            handle.write(raw)
    session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:12]
    session = run / "sessions" / session_id
    session.mkdir(parents=True)
    metadata = {"run_id": run_id, "session_id": session_id, "started_ms": now_ms(),
                "rules_sha256": digest, "host": socket.gethostname(), "python": sys.version,
                "mode": "record_only", "schema_version": 1}
    if not (run / "run.json").exists():
        (run / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (session / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return Store(session, session_id=session_id), metadata


def append(store, path, kind, row):
    return store.append("nba", kind, row, path)
