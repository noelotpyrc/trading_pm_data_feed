"""pm_add_asym_probe: synthetic frames, no network. Run: python -m pytest tests/test_pm_add_asym_probe.py -q"""
import argparse
import json
import math

from pm_add_asym_probe.__main__ import Out, read_csv, replay
from pm_add_asym_probe.add_asym import walk
from pm_add_asym_probe.engine import Engine

T, UP, DN = 1_000_000_200 // 900 * 900, "u", "d"


def book(a, ts, bids, asks):
    return json.dumps({"event_type": "book", "asset_id": a, "timestamp": str(int(ts * 1000)),
                       "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
                       "asks": [{"price": str(p), "size": str(s)} for p, s in asks]})


def pc(a, ts, side, p, s, bb, ba):
    return json.dumps({"event_type": "price_change", "timestamp": str(int(ts * 1000)), "price_changes": [
        {"asset_id": a, "price": str(p), "size": str(s), "side": side, "best_bid": str(bb), "best_ask": str(ba)}]})


def lines():
    """Up book 0.30/0.31; ask-side adds near the best every second from 700 s -> add_asym_30 = -1 for the Up token."""
    L = [{"r": T - 1, "c": 1, "sub": [{"T": T, "up": UP, "down": DN}]},
         {"r": T + 600.05, "c": 1, "m": book(UP, T + 600, [(0.30, 100), (0.29, 100)], [(0.31, 5), (0.32, 20)])}]
    for k, s in enumerate(range(700, 800)):
        L.append({"r": T + s + 0.55, "c": 1, "m": pc(UP, T + s + 0.5, "SELL", 0.31, 5 + k + 1, 0.30, 0.31)})
    return L


def run(cool=30, lo=0.05, hi=0.50):
    e = Engine(-0.95, lo, hi, cool)
    for x in lines():
        e.on_line(x)
    e.tick(T + 900)
    return e


def test_walk():
    assert walk({0.31: 5, 0.32: 20}, 10, buy=True) == (5 * 0.31 + 5 * 0.32) / 10
    assert walk({0.30: 4, 0.29: 6}, 10, buy=False) == (4 * 0.30 + 6 * 0.29) / 10
    assert math.isnan(walk({0.31: 5}, 10, buy=True))


def test_seconds_and_values():
    e = run()
    S = {r["s"]: r for r in e.seconds}
    assert sorted(S) == list(range(720, 896))
    assert S[720]["v_up"] == -1.0 and S[720]["bb"] == 0.30 and S[720]["ba"] == 0.31
    # second 720 is emitted before the add at 720.5 is applied: last add counted is at 719.5 (size 5 + 20)
    assert S[720]["buy_up"] == 0.31                                 # 25 shares at 0.31 >= 10
    assert S[720]["buy_down"] == 1 - 0.30
    assert math.isnan(S[830]["v_up"])                                # last add at 799.5 drops out at 829.5
    assert e.n["late"] == 0


def test_policy_cooldown_and_limits():
    e = run(cool=30)
    assert [(x["s"], x["token"]) for x in e.entries] == [(720, "up"), (750, "up"), (780, "up"), (810, "up")]
    assert all(x["e"] == 0.31 and x["value"] == -1.0 for x in e.entries)
    assert len(run(cool=0).entries) == 110                           # 720..829, the Down side (value +1) never fires
    assert run(lo=0.35).entries == []


def test_late_and_gap():
    e = Engine(-0.95, 0.05, 0.5, 30)
    for x in lines()[:2]:
        e.on_line(x)
    e.tick(T + 760)                                                  # timer emits 720..758
    e.on_line({"r": T + 760.1, "c": 1, "m": pc(UP, T + 750, "SELL", 0.31, 50, 0.30, 0.31)})
    assert e.n["late"] == 1
    e.on_line({"r": T + 761, "c": 1, "close": "error"})
    e.on_line({"r": T + 762, "c": 2, "close": "error"})            # failed reconnect keeps the first close time
    e.on_line({"r": T + 765, "c": 3, "sub": [{"T": T, "up": UP, "down": DN}]})
    e.on_line({"r": T + 765.2, "c": 3, "m": book(UP, T + 765.1, [(0.30, 1)], [(0.31, 1)])})
    assert e.gaps == [(T + 761, T + 765.2)]
    e.tick(T + 900)
    g = {r["s"]: r["gap"] for r in e.seconds}
    assert g[761] == 0 and g[762] == 1 and g[795] == 1 and g[796] == 0      # 762..764 emitted while the gap was open


def test_replay_matches(tmp_path):
    (tmp_path / "run.json").write_text(json.dumps({"cut": -0.95, "lo": 0.05, "hi": 0.5, "cool": 30}))
    e, out = Engine(-0.95, 0.05, 0.5, 30), Out(tmp_path)
    for x in lines():
        out.record(x)
        e.on_line(x)
        out.drain(e)
    e.tick(T + 900)                                                  # live timer ticks past the last frame
    out.drain(e)
    out.close()
    replay(argparse.Namespace(data_dir=str(tmp_path), no_timer=False))
    A, B = read_csv(tmp_path / "seconds.csv"), read_csv(tmp_path / "replay" / "seconds.csv")
    assert len(B) and all({k: v for k, v in a.items() if k != "wall"} == {k: v for k, v in b.items() if k != "wall"} for a, b in zip(A, B))
    E, F = read_csv(tmp_path / "entries.csv"), read_csv(tmp_path / "replay" / "entries.csv")
    assert F and [x["s"] for x in F] == [x["s"] for x in E][:len(F)]   # replay has no timer ticks after the last line


def test_bad_message_counted():
    e = Engine(-0.95, 0.05, 0.5, 30)
    e.on_line(lines()[0])
    e.on_line({"r": T + 1, "c": 1, "m": json.dumps({"event_type": "book", "asset_id": UP, "timestamp": "1", "bids": [{"size": "1"}]})})
    e.on_line({"r": T + 2, "c": 1, "m": "{not json"})
    assert e.n["bad"] == 2


def test_replay_unclosed_file_and_no_timer(tmp_path):
    (tmp_path / "run.json").write_text(json.dumps({"cut": -0.95, "lo": 0.05, "hi": 0.5, "cool": 30}))
    e, out = Engine(-0.95, 0.05, 0.5, 30), Out(tmp_path)
    for x in lines():
        out.record(x)
        e.on_line(x)
        out.drain(e)
    out.flush()                                                      # no close: the hour file has no gzip end marker
    replay(argparse.Namespace(data_dir=str(tmp_path), no_timer=False))
    replay(argparse.Namespace(data_dir=str(tmp_path), no_timer=True))
    assert len(read_csv(tmp_path / "replay" / "seconds.csv")) == 80          # 720..799 emitted by events or the timer
    assert len(read_csv(tmp_path / "replay_no_timer" / "seconds.csv")) == 80 # here every second has a later event
