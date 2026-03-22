# Consumer Integration Plan

Agreed boundary between this project (VPS data pipeline) and the local trading node.

## Current Operational Boundary

This VPS repo owns:
- 1m BTCUSDT perp source accumulation
- Daily artifact generation
- Parity validation against historical truth
- VPS/local transfer utilities

The trading repo assumes:
- Source-of-truth OHLCV lives remotely on the VPS
- The preferred input to the local trading node is the pulled daily artifact bundle
- Pulling the raw SQLite DB is mainly for debugging, parity checks, or emergency rebuilds

## Expected Local Consumer Behavior

Local node setup should follow this sequence:

1. Verify artifact availability on VPS
2. Pull the required artifact day locally
3. Validate presence of:
   - `model.json`
   - `z_pool.npy`
   - `metadata.json`
4. Start the local node using the local artifact copy

The local node should not depend on live remote DB access during normal startup.

## Current Local Integration Decisions

- For now, artifact pull can be initiated by the local node startup path
- However, the pull logic should be abstracted behind a separate local component so it can later be moved to a standalone preflight/sync process
- If the required artifact is missing or invalid, local startup should **fail closed** for now
- Future fallback logic, if added, should live in that standalone sync/preflight layer rather than inside the node itself
- Pulling the raw SQLite DB should remain manual/debug-oriented for now, not part of normal node startup

## Design Boundary

**This repo** (VPS data pipeline) owns:
- Source-data transport
- DB recovery
- Artifact fallback rebuilds
- Cross-machine sync policy

**The trading repo** stays focused on:
- Consuming a local artifact bundle
- Validating artifact presence/shape
- Running the trading node against local inputs

The trading repo should not become the primary owner of raw-source replication or remote-data incident tooling.

## Notes for Future Implementation

- Local code should treat missing artifact bundle as a hard startup problem unless an explicit fallback path is designed
- If a fallback is ever added, it should rebuild from a pulled local SQLite copy, not query the VPS SQLite file directly
- The trading repo does not need to own VPS maintenance concerns like cron, retention, or log rotation
