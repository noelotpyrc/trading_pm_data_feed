"""Copy an append-only run through a flushed, consistent byte boundary."""
import json
import os
from pathlib import Path
import uuid

from .storage import now_ms


def backup_run(store, destination, stop):
    source = store.root.parents[1]  # <run>/sessions/<session>
    destination = Path(destination).resolve()
    if not destination.is_dir():
        raise ValueError("Backup destination must already exist on the mounted SSD")
    if destination.is_relative_to(source) or source.is_relative_to(destination):
        raise ValueError("Backup destination and source must not overlap")
    with store.lock:
        store.flush()
        # Only lengths are captured under the writer lock. The slow SSD copy
        # reads those immutable file prefixes without blocking live recording.
        files = []
        for path in sorted(source.rglob("*")):
            if path.is_symlink():
                raise ValueError("Unexpected symlink in recording")
            if path.is_file():
                files.append((path, path.relative_to(source), path.stat().st_size))
        snapshot_ms = now_ms()
    target = destination / source.name
    if target.is_symlink() or not target.resolve().is_relative_to(destination):
        raise ValueError("Backup run directory escapes destination")
    target.mkdir(parents=True, exist_ok=True)
    for path, relative, length in files:
        if stop.is_set():
            return None
        output = target / relative
        if not output.resolve().is_relative_to(target.resolve()):
            raise ValueError("Backup target escapes destination")
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(output.name + "." + uuid.uuid4().hex + ".partial")
        try:
            with path.open("rb") as reader, temporary.open("xb") as writer:
                remaining = length
                while remaining:
                    if stop.is_set():
                        return None
                    chunk = reader.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise OSError(f"Recording shrank during backup: {relative}")
                    writer.write(chunk)
                    remaining -= len(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
    manifest = {"recv_ms": snapshot_ms, "kind": "backup", "session_id": store.session_id,
                "files": {str(relative): size for _, relative, size in files}}
    temporary = target / ("backup-" + uuid.uuid4().hex + ".partial")
    try:
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        os.replace(temporary, target / "backup.json")
    finally:
        temporary.unlink(missing_ok=True)
    return manifest
