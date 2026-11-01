"""Verified live-prefix backup from the local Mac recorder; never removes source data.

Each batch is a standalone snapshot of the files currently on the Mac. Preserve
earlier batches for historical data no longer present in a later snapshot.
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile

INVENTORY = r'''
import hashlib,json,sys,os
from pathlib import Path
root=Path(json.load(sys.stdin)['root']).resolve();files=[]
assert root.is_dir()
def record(path):
 if not path.is_file() or path.name.startswith('.'):return
 assert not path.is_symlink() and path.resolve().is_relative_to(root)
 n=path.stat().st_size
 if path.suffix=='.jsonl' and n:
  with path.open('rb') as f:
   end=n
   while end:
    start=max(0,end-65536);f.seek(start);data=f.read(end-start);i=data.rfind(b'\n')
    if i>=0:n=start+i+1;break
    end=start
   else:n=0
 h=hashlib.sha256();left=n
 with path.open('rb') as f:
  while left:
   b=f.read(min(left,1024*1024))
   if not b:raise RuntimeError('Source truncated during inventory')
   left-=len(b);h.update(b)
 files.append({'key':str(path.relative_to(root)),'bytes':n,'sha256':h.hexdigest()})
# Freeze all JSONL reference prefixes before enumerating immutable payloads.
# A payload is durably written before its reference becomes visible.
for directory,dirs,names in os.walk(root):
 dirs[:]=sorted(d for d in dirs if d!='payloads')
 for name in sorted(names):record(Path(directory)/name)
for path in sorted(root.rglob('payloads/**/*.json.gz')):record(path)
print(json.dumps({'root':str(root),'files':files}))
'''

TRANSFER = r'''
import json,sys,tarfile,gzip
from pathlib import Path,PurePosixPath
manifest=json.load(sys.stdin);root=Path(manifest['root']).resolve()
class Prefix:
 def __init__(self,f,n):self.f,self.left=f,n
 def read(self,n=-1):
  n=self.left if n<0 else min(n,self.left);b=self.f.read(n)
  if len(b)!=n:raise RuntimeError('Source truncated during transfer')
  self.left-=len(b);return b
with gzip.GzipFile(fileobj=sys.stdout.buffer,mode='wb',compresslevel=1) as compressed, tarfile.open(fileobj=compressed,mode='w|') as tar:
 for row in manifest['files']:
  key=PurePosixPath(row['key']);assert not key.is_absolute() and '..' not in key.parts
  p=root/row['key'];assert not p.is_symlink() and p.resolve().is_relative_to(root)
  st=p.stat();assert st.st_size>=row['bytes']
  info=tarfile.TarInfo(row['key']);info.size=row['bytes'];info.mode=0o600;info.mtime=st.st_mtime
  with p.open('rb') as f:tar.addfile(info,Prefix(f,row['bytes']))
'''


def remote(host, code):
    return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', host,
            'exec /usr/bin/nice -n 10 /usr/bin/python3 -c ' + shlex.quote(code)]


def verify(archive, manifest):
    expected = {r['key']: r for r in manifest['files']}
    seen, payloads, references = set(), set(), set()
    with tarfile.open(archive, 'r|gz') as tar:
        for member in tar:
            assert member.name in expected and member.name not in seen and member.isfile()
            row = expected[member.name]
            assert member.size == row['bytes']
            stream = tar.extractfile(member)
            h = hashlib.sha256(); size = 0
            retain = '/payloads/' in member.name and member.name.endswith('.json.gz')
            refs = '/observations/' in member.name or '/book_windows/' in member.name
            chunks = [] if retain or refs else None
            for b in iter(lambda: stream.read(1024 * 1024), b''):
                h.update(b); size += len(b)
                if chunks is not None: chunks.append(b)
            assert (size, h.hexdigest()) == (row['bytes'], row['sha256'])
            seen.add(member.name)
            if retain:
                raw = gzip.decompress(b''.join(chunks)); json.loads(raw)
                assert hashlib.sha256(raw).hexdigest() == Path(member.name).name[:-8]
                payloads.add(member.name)
            elif refs:
                for line in b''.join(chunks).splitlines():
                    r = json.loads(line)
                    if '/observations/' in member.name and r.get('raw_payload_path'):
                        session = member.name.split('/observations/')[0]
                        references.add(session + '/' + r['raw_payload_path'])
                    snap = r.get('fotmob_snapshot') or {}
                    if '/book_windows/' in member.name and snap.get('raw_payload_path'):
                        references.add('fotmob/sessions/' + r['fotmob_shadow_session'] + '/' + snap['raw_payload_path'])
    assert seen == set(expected)
    assert references <= payloads, 'Snapshot missing payload references: ' + str(sorted(references - payloads)[:5])
    return {'file_count': len(seen), 'payloads_verified': len(payloads), 'payload_references_verified': len(references)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='m1max')
    parser.add_argument('--root', default='/Users/leon/services/pm-soccer/recordings')
    parser.add_argument('--dest', type=Path, required=True)
    args = parser.parse_args(argv)
    destination = args.dest.absolute()
    if destination.parts[1:2] == ('Volumes',):
        assert Path(*destination.parts[:3]).is_mount(), 'Destination SSD is not mounted'
    snapshot = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    response = subprocess.run(remote(args.host, INVENTORY), input=json.dumps({'root': args.root}),
                              capture_output=True, text=True, check=True)
    manifest = json.loads(response.stdout)
    assert manifest['files'], 'No recordings found'
    manifest.update(source_host=args.host, snapshot=snapshot,
                    method='Standalone fixed-prefix snapshot; JSONL ends at a complete row. Source keeps recording. Not atomic across files.')
    batch = destination / snapshot; batch.mkdir(parents=True, exist_ok=False)
    manifest_path = batch / 'BACKUP_MANIFEST.json'
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    partial = batch / 'recordings.tar.gz.partial'
    with partial.open('xb') as out:
        p = subprocess.run(remote(args.host, TRANSFER), input=json.dumps(manifest).encode(), stdout=out, check=True)
        out.flush(); os.fsync(out.fileno())
    archive = batch / 'recordings.tar.gz'
    receipt = verify(partial, manifest)
    os.replace(partial, archive)
    h = hashlib.sha256()
    with archive.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''): h.update(b)
    receipt.update(verified_utc=datetime.now(timezone.utc).isoformat(), sha256_verified=True,
                   source_prefix_bytes=sum(r['bytes'] for r in manifest['files']), archive_bytes=archive.stat().st_size,
                   archive_sha256=h.hexdigest(), manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                   source_data_removed=False)
    with (batch / 'BACKUP_VERIFICATION.json').open('x') as f:
        json.dump(receipt, f, indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
    print(json.dumps({'batch': str(batch), **receipt}, indent=2))


if __name__ == '__main__':
    main()
