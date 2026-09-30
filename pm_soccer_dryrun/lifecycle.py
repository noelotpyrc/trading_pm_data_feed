"""Discovery admission and collection expiry shared by the live workers."""

HOUR_MS = 3_600_000
DISCOVERY_LOOKBACK_MS = 4 * HOUR_MS
DISCOVERY_LOOKAHEAD_MS = 7 * 24 * HOUR_MS


def in_discovery_window(kickoff_ms, at_ms):
    return at_ms - DISCOVERY_LOOKBACK_MS <= kickoff_ms <= at_ms + DISCOVERY_LOOKAHEAD_MS


def collection_end(match):
    """Return (end_ms, reason), or (None, None) while lifecycle data is awaited.

    A feed ID alone does not demonstrate score coverage. Accepted score data,
    a final status, or a market close disarms the no-evidence timeout.
    """
    if match.get("collection_expired_ms") is not None:
        return match["collection_expired_ms"], "no_feed_or_close"
    full_time, closed = match.get("full_time_ms"), match.get("all_closed_ms")
    if full_time is not None and closed is not None:
        return max(closed, full_time + 30 * 60_000), "finished_and_closed"
    any_closed = closed is not None or any(
        b.get("closed") or b.get("closed_ms") is not None for b in match.get("books", {}).values())
    if match.get("score_observed_ms") is None and full_time is None and not any_closed:
        return match["kickoff_ms"] + DISCOVERY_LOOKBACK_MS, "no_feed_or_close"
    return None, None
