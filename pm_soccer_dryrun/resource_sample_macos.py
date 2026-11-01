"""Read-only launchd/ps resource samples; no service control or retention actions."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess


def cpu_ns(text):
    days, clock = text.split('-', 1) if '-' in text else ('0', text)
    parts = clock.split(':')
    if len(parts) not in (2, 3):
        raise ValueError('Unexpected ps CPU time: ' + text)
    seconds = float(parts[-1]) + int(parts[-2]) * 60
    if len(parts) == 3:
        seconds += int(parts[0]) * 3600
    return round((int(days) * 86400 + seconds) * 1_000_000_000)


def run(command, *, check=True):
    return subprocess.run(command, capture_output=True, text=True, check=check, timeout=25)


def job(label, uid):
    for domain in ('system', 'gui/' + str(uid)):
        response = run(['/bin/launchctl', 'print', domain + '/' + label], check=False)
        if response.returncode:
            continue
        fields = {}
        for name in ('state', 'pid', 'runs', 'last exit code'):
            match = re.search(r'^\s*' + name + r' = ([^\n]+)', response.stdout, re.M)
            fields[name] = match[1].strip() if match else None
        pid = int(fields['pid']) if fields['pid'] and fields['pid'].isdigit() else None
        return domain, fields, pid
    return None, {}, None


def host_memory():
    text = run(['/usr/bin/vm_stat']).stdout
    page_size = int(re.search(r'page size of (\d+) bytes', text)[1])
    pages = {key: int(value) for key, value in re.findall(r'^([^:\n]+):\s+(\d+)\.', text, re.M)}
    pressure = run(['/usr/bin/memory_pressure', '-Q']).stdout
    percent = re.search(r'free percentage:\s*(\d+)%', pressure)
    swap = run(['/usr/sbin/sysctl', 'vm.swapusage']).stdout
    used = re.search(r'used = ([\d.]+)([KMG])', swap)
    return {
        'host_memory_free_bytes': pages.get('Pages free', 0) * page_size,
        'host_memory_inactive_bytes': pages.get('Pages inactive', 0) * page_size,
        'host_memory_purgeable_bytes': pages.get('Pages purgeable', 0) * page_size,
        'host_memory_compressed_bytes': pages.get('Pages occupied by compressor', 0) * page_size,
        'host_memory_pressure_free_percent': int(percent[1]) if percent else None,
        'host_available_memory_bytes': None,  # macOS has no Linux MemAvailable equivalent here.
        'host_swap_used_bytes': round(float(used[1]) * 1024 ** ('KMG'.index(used[2]) + 1)) if used else None,
        'host_load_average': list(os.getloadavg()),
    }


def sample(target, uid, host, allocated):
    label, root = target['label'], Path(target['data_dir'])
    domain, fields, pid = job(label, uid)
    row = {
        'observed_at': datetime.now(timezone.utc).isoformat(), 'platform': 'darwin',
        'unit': label, 'role': target['role'], 'launchd_domain': domain,
        'service_state': fields.get('state', 'not_loaded'), 'recorder_pid': pid,
        'launchd_runs': int(fields['runs']) if (fields.get('runs') or '').isdigit() else None,
        'last_exit_code': fields.get('last exit code'), 'cpu_usage_ns': None,
        'cpu_time_precision_ns': 10_000_000, 'rss_bytes': None, 'peak_rss_bytes': None,
        'process_swap_bytes': None, 'disk_free_bytes': shutil.disk_usage(root).free,
        'run_allocated_bytes': allocated[str(root)], **host,
    }
    if pid:
        response = run(['/bin/ps', '-p', str(pid), '-o', 'pid=,rss=,time='], check=False)
        parts = response.stdout.split()
        if response.returncode == 0 and len(parts) == 3 and int(parts[0]) == pid:
            row.update(rss_bytes=int(parts[1]) * 1024, cpu_usage_ns=cpu_ns(parts[2]))
        else:
            row.update(recorder_pid=None, service_state='exited_during_sample')
    return row


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args(argv)
    config = json.loads(args.config.read_text())
    host, allocated = host_memory(), {}
    for target in config['targets']:
        root = Path(target['data_dir'])
        if str(root) not in allocated:
            allocated[str(root)] = int(run(['/usr/bin/du', '-sk', str(root)]).stdout.split()[0]) * 1024
    for target in config['targets']:
        row = sample(target, config['uid'], host, allocated)
        output = Path(target['data_dir']) / 'resources' / (row['observed_at'][:10] + '.jsonl')
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open('a') as handle:
            handle.write(json.dumps(row, separators=(',', ':')) + '\n')
        print(json.dumps(row), flush=True)


if __name__ == '__main__':
    main()
