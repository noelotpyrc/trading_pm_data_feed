import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile

import pytest

spec = importlib.util.spec_from_file_location('soccer_mac_backup', Path(__file__).parents[1] / 'utils/backup_soccer_macos.py')
backup = importlib.util.module_from_spec(spec); spec.loader.exec_module(backup)


def archive(tmp_path, *, omit_payload=False, bad_hash=False):
    raw = b'{"header":{"status":"ok"}}'
    digest = hashlib.sha256(raw).hexdigest()
    payload_key = 'fotmob/sessions/session/payloads/' + digest[:2] + '/' + digest + '.json.gz'
    obs_key = 'fotmob/sessions/session/observations/2026-10-20.jsonl'
    files = {obs_key: (json.dumps({'raw_payload_path': payload_key.split('/session/')[1]}) + '\n').encode()}
    if not omit_payload:files[payload_key] = gzip.compress(raw)
    rows = []
    with tarfile.open(tmp_path / 'backup.tar.gz', 'w:gz') as tar:
        for key, data in files.items():
            path = tmp_path / key; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
            tar.add(path, arcname=key)
            rows.append({'key': key, 'bytes': len(data), 'sha256': '0' * 64 if bad_hash else hashlib.sha256(data).hexdigest()})
    return tmp_path / 'backup.tar.gz', {'files': rows}


def test_backup_verifies_contents_and_payload_reference_closure(tmp_path):
    path, manifest = archive(tmp_path)
    assert backup.verify(path, manifest) == {'file_count': 2, 'payloads_verified': 1, 'payload_references_verified': 1}


def test_backup_rejects_missing_referenced_payload(tmp_path):
    path, manifest = archive(tmp_path, omit_payload=True)
    with pytest.raises(AssertionError, match='missing payload'):
        backup.verify(path, manifest)


def test_backup_rejects_changed_recorded_bytes(tmp_path):
    path, manifest = archive(tmp_path, bad_hash=True)
    with pytest.raises(AssertionError):backup.verify(path, manifest)
