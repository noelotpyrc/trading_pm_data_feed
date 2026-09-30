"""Linux/systemd resource snapshots only; never stops services or deletes data."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess


def kib_fields(path):
    return {key: int(value.split()[0]) * 1024
            for line in path.read_text().splitlines() if ':' in line
            for key, value in [line.split(':', 1)] if value.strip().endswith('kB')}


def sample(unit, data_dir, proc_root=Path('/proc')):
    names = ['MainPID', 'ActiveState', 'SubState', 'NRestarts', 'CPUUsageNSec', 'MemoryCurrent', 'MemoryPeak']
    response = subprocess.run(['systemctl', 'show', unit, '--property=' + ','.join(names)],
                              check=True, capture_output=True, text=True, timeout=10)
    properties = dict(line.split('=', 1) for line in response.stdout.splitlines() if '=' in line)
    def number(name):
        value = properties.get(name, '')
        return int(value) if value.isdigit() and int(value) < 2**64 - 1 else None
    pid = number('MainPID') or 0
    process = {}
    if pid:
        try:
            process = kib_fields(proc_root / str(pid) / 'status')
        except FileNotFoundError:
            pass  # The process can exit between the systemd query and this read.
    host = kib_fields(proc_root / 'meminfo')
    disk = shutil.disk_usage(data_dir)
    # The recorder only appends. Retention/deletion belongs to the backup workflow.
    usage = subprocess.run(['du', '-s', '-B1', str(data_dir)],
                           check=True, capture_output=True, text=True, timeout=20)
    return {'observed_at': datetime.now(timezone.utc).isoformat(), 'unit': unit,
            'service_state': properties.get('ActiveState'), 'service_substate': properties.get('SubState'),
            'recorder_pid': pid or None, 'service_restarts': number('NRestarts'),
            'cpu_usage_ns': number('CPUUsageNSec'), 'cgroup_memory_bytes': number('MemoryCurrent'),
            'cgroup_memory_peak_bytes': number('MemoryPeak'), 'rss_bytes': process.get('VmRSS'),
            'peak_rss_bytes': process.get('VmHWM'), 'process_swap_bytes': process.get('VmSwap'),
            'host_available_memory_bytes': host.get('MemAvailable'),
            'host_swap_used_bytes': host['SwapTotal'] - host['SwapFree'],
            'disk_free_bytes': disk.free, 'run_allocated_bytes': int(usage.stdout.split()[0])}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--unit', default='pm-soccer@live.service')
    parser.add_argument('--data-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    row = sample(args.unit, args.data_dir)
    output = args.data_dir / 'resources' / (row['observed_at'][:10] + '.jsonl')
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('a') as handle:
        handle.write(json.dumps(row, separators=(',', ':')) + '\n')
    print(json.dumps(row))


if __name__ == '__main__':
    main()
