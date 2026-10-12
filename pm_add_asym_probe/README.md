# pm_add_asym_probe — record + paper signal, BTC 15m up/down

Records every Polymarket market-channel WS frame for the current and next BTC 15-minute window, and computes
`add_asym_30` per second (seconds 720–895) with paper entries held to settlement. No orders, no positions.
Thresholds are command-line arguments only; the run writes them into `run.json`.

Needs Python 3.10+ and `websocket-client`. From the repository root:

```sh
caffeinate -i uv run --no-project --python 3.12 --with websocket-client python -m pm_add_asym_probe live \
  --data-dir ~/pm_add_asym_probe/<run id> --cut <C> --lo <L> --hi <H> --cool <S> --hours <N>
python -m pm_add_asym_probe replay --data-dir <D>    # rebuild D/replay/*.csv from D/raw, diff vs the live CSVs
python -m pm_add_asym_probe replay --data-dir <D> --no-timer   # same, research order (no 1 s timer) -> D/replay_no_timer
python -m pm_add_asym_probe settle --data-dir <D>    # Gamma outcomes -> D/settled.csv + summary
```

- Subscription moves to (current, next) window at second 300 of each window; drops (or 30 s with no frame) reconnect after 1–5 s.
- `raw/<UTC hour>.jsonl.gz`: `{"r": recv time, "c": connection, "sub" | "m" | "close": ...}` per line, flushed every 10 s.
- `seconds.csv`: one row per window second 720–895; `gap` = 1 when a connection gap overlapped the last 30 s.
- `entries.csv`: paper entries; `delay` = wall clock at the decision − the second it is for.
- `run.log`: a status line per minute (frames, levels, books, bad messages, late events, reconnects, gaps, median receive lag).
- The engine (`engine.py`) is shared by `live` and `replay`, so a replay reproduces the live CSVs except wall-clock fields.

Tests: `python -m pytest tests/test_pm_add_asym_probe.py -q`
