from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_soccer_dryrun import resource_sample


@pytest.mark.parametrize('running,vanished', [(True, False), (True, True), (False, False)])
def test_resource_sample_records_usage_without_controlling_service(tmp_path, monkeypatch, running, vanished):
    proc = tmp_path / 'proc'
    proc.mkdir()
    (proc / 'meminfo').write_text('MemAvailable: 400000 kB\nSwapTotal: 100000 kB\nSwapFree: 90000 kB\n')
    if running and not vanished:
        (proc / '42').mkdir()
        (proc / '42/status').write_text('Name:\tpython\nVmRSS:\t120000 kB\nVmHWM:\t150000 kB\nVmSwap:\t2000 kB\n')
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if command[0] == 'systemctl':
            assert command[1:3] == ['show', 'pm-soccer@live.service']
            return SimpleNamespace(stdout=f'MainPID={42 if running else 0}\nActiveState={"active" if running else "inactive"}\nSubState=running\nNRestarts=0\nCPUUsageNSec=123456789\nMemoryCurrent=123000000\nMemoryPeak=[not set]\n')
        assert command == ['du', '-s', '-B1', str(tmp_path)]
        return SimpleNamespace(stdout=f'8192\t{tmp_path}\n')
    monkeypatch.setattr(resource_sample.subprocess, 'run', run)
    monkeypatch.setattr(resource_sample.shutil, 'disk_usage', lambda _: SimpleNamespace(free=9999999))
    result = resource_sample.sample('pm-soccer@live.service', tmp_path, proc)
    assert result['host_available_memory_bytes'] == 400000 * 1024
    assert result['host_swap_used_bytes'] == 10000 * 1024
    assert result['run_allocated_bytes'] == 8192 and result['disk_free_bytes'] == 9999999
    assert result['cgroup_memory_peak_bytes'] is None
    assert result['rss_bytes'] == (120000 * 1024 if running and not vanished else None)
    assert result['recorder_pid'] == (42 if running else None)
    assert len(calls) == 2
