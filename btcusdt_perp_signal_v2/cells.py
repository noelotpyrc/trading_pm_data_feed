"""
Regime gate + cell firing — SPEC_book25_fcfs.md §4/§6.1, SPEC_dryrun_book11.md §3.

fires_c[t] = regime[t] AND all( decile_{f,W_c}[t] == required for (f, required) in legs_c ).
A NaN (None) decile on any leg makes the cell not fire; a NaN regime decile makes nothing fire.
"""
from __future__ import annotations

from btcusdt_perp_signal_v2 import config
from btcusdt_perp_signal_v2.config import Cell


def regime_decile(deciles: dict):
    return deciles.get((config.REGIME_FEATURE, config.REGIME_WINDOW))


def in_regime(deciles: dict) -> bool:
    rd = regime_decile(deciles)
    return rd is not None and rd in config.REGIME_DECILES


def leg_detail(cell: Cell, deciles: dict) -> list:
    """[(feature, actual_decile, required_decile), ...] for the FIRING record."""
    return [(feat, deciles.get((feat, cell.window)), req) for feat, req in cell.legs]


def cell_fires(cell: Cell, deciles: dict) -> bool:
    for feat, req in cell.legs:
        if deciles.get((feat, cell.window)) != req:
            return False
    return True


def firing_cells(deciles: dict) -> list:
    """Cells that fire this bar (regime + all legs), in book (ascending id) order."""
    if not in_regime(deciles):
        return []
    return [c for c in config.CELLS if cell_fires(c, deciles)]
