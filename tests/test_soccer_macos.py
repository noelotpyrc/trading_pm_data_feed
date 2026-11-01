from pathlib import Path
import plistlib
from types import SimpleNamespace

import pytest

from pm_soccer_dryrun import macos_services as jobs, resource_sample_macos as resources


@pytest.mark.parametrize('text,seconds', [('00:01.25', 1.25), ('13:12.50', 792.5),
                                         ('01:02:03.00', 3723), ('2-01:02:03.50', 176523.5)])
def test_ps_cpu_time(text, seconds):
    assert resources.cpu_ns(text) == round(seconds * 1_000_000_000)


def test_launchd_sampler_falls_back_to_user_domain(monkeypatch):
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if command[-1].startswith('system/'):
            return SimpleNamespace(returncode=113, stdout='')
        return SimpleNamespace(returncode=0, stdout=' state = running\n pid = 42\n runs = 2\n last exit code = 0\n')
    monkeypatch.setattr(resources, 'run', run)
    domain, fields, pid = resources.job('com.leon.test', 501)
    assert domain == 'gui/501' and pid == 42 and fields['runs'] == '2'
    assert calls == [['/bin/launchctl', 'print', 'system/com.leon.test'],
                     ['/bin/launchctl', 'print', 'gui/501/com.leon.test']]


@pytest.mark.parametrize('vanished', [False, True])
def test_macos_sample_records_actual_cpu_and_rss_or_process_exit(tmp_path, monkeypatch, vanished):
    monkeypatch.setattr(resources, 'job', lambda *a: ('system', {'state': 'running', 'runs': '3'}, 42))
    monkeypatch.setattr(resources.shutil, 'disk_usage', lambda p: SimpleNamespace(free=100000))
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=1 if vanished else 0, stdout='' if vanished else '42 120000 02:00.50\n')
    monkeypatch.setattr(resources, 'run', run)
    row = resources.sample({'label': 'com.leon.test', 'role': 'primary', 'data_dir': str(tmp_path)},
                           501, {'host_swap_used_bytes': 1024}, {str(tmp_path): 8192})
    assert row['rss_bytes'] == (None if vanished else 120000 * 1024)
    assert row['cpu_usage_ns'] == (None if vanished else 120500000000)
    assert row['recorder_pid'] == (None if vanished else 42)
    assert row['run_allocated_bytes'] == 8192 and row['peak_rss_bytes'] is None
    assert calls == [['/bin/ps', '-p', '42', '-o', 'pid=,rss=,time=']]


def test_macos_memory_does_not_mislabel_cache_as_memavailable(monkeypatch):
    def run(command, **kwargs):
        if command[0].endswith('vm_stat'):
            return SimpleNamespace(stdout='Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 10.\nPages inactive: 20.\nPages purgeable: 3.\nPages occupied by compressor: 40.\n')
        if command[0].endswith('memory_pressure'):
            return SimpleNamespace(stdout='System-wide memory free percentage: 46%\n')
        return SimpleNamespace(stdout='vm.swapusage: total = 1024.00M used = 209.94M free = 814.06M (encrypted)\n')
    monkeypatch.setattr(resources, 'run', run)
    row = resources.host_memory()
    assert row['host_memory_free_bytes'] == 10 * 16384
    assert row['host_memory_compressed_bytes'] == 40 * 16384
    assert row['host_memory_pressure_free_percent'] == 46
    assert row['host_available_memory_bytes'] is None
    assert row['host_swap_used_bytes'] == round(209.94 * 1024 ** 2)


def test_job_topology_keeps_same_recording_contract_and_no_tunnels(tmp_path):
    configs = jobs.configs(tmp_path, 'leon', 'soccer-m1max-test')
    assert len(configs) == 5
    for config in configs.values():
        assert plistlib.loads(plistlib.dumps(config)) == config
        assert config['ProgramArguments'][0] == str(tmp_path / '.venv/bin/python')
        assert '/usr/bin/ssh' not in config['ProgramArguments']
        assert config['RunAtLoad']
    primary = configs['primary']['ProgramArguments']
    for flag, value in [('--score-relay-bind', '127.0.0.1'), ('--score-relay-peer', '127.0.0.1'),
                        ('--book-window-minutes', '5'), ('--fotmob-shadow-dir', str(tmp_path / 'recordings/fotmob'))]:
        assert primary[primary.index(flag) + 1] == value
    assert configs['resources']['StartInterval'] == 60
    assert 'KeepAlive' not in configs['resources']
    assert configs['primary']['KeepAlive']


def test_system_install_requires_root_before_touching_jobs(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs.os, 'geteuid', lambda: 501)
    monkeypatch.setattr(jobs, 'launch', lambda *a, **k: pytest.fail('Must not control services without root'))
    with pytest.raises(PermissionError, match='require sudo'):
        jobs.install_system(tmp_path, 'leon', {}, None)


def test_user_install_rejects_existing_unrelated_plist(tmp_path, monkeypatch):
    account = SimpleNamespace(pw_uid=501, pw_dir=str(tmp_path))
    agents = tmp_path / 'Library/LaunchAgents'; agents.mkdir(parents=True)
    templates = tmp_path / 'launchagents'; templates.mkdir()
    label = 'com.leon.pm-soccer-primary'
    (agents / (label + '.plist')).write_bytes(b'existing-other-job')
    (templates / (label + '.plist')).write_bytes(b'new-job')
    calls = []
    def launch(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=113 if args[-1].startswith('system/') else 0)
    monkeypatch.setattr(jobs.os, 'getuid', lambda: 501)
    monkeypatch.setattr(jobs, 'launch', launch)
    with pytest.raises(AssertionError, match='Existing unrelated'):
        jobs.install_user(tmp_path, 'leon', {'primary': {'Label': label}}, account)
    assert (agents / (label + '.plist')).read_bytes() == b'existing-other-job'
    assert all(args[0] == 'print' for args in calls)


def test_system_migration_rolls_back_agents_if_bootstrap_fails(tmp_path, monkeypatch):
    root = tmp_path / 'service'; root.mkdir()
    home = tmp_path / 'home'; agents = home / 'Library/LaunchAgents'; agents.mkdir(parents=True)
    daemons = tmp_path / 'system'; daemons.mkdir()
    templates = root / 'launchdaemons'; templates.mkdir()
    user_templates = root / 'launchagents'; user_templates.mkdir()
    configs = {k: v for k, v in jobs.configs(root, 'leon', 'test').items() if k in ('primary', 'score-worker')}
    original = {}
    for config in configs.values():
        name = config['Label'] + '.plist'
        original[name] = plistlib.dumps(config)
        (agents / name).write_bytes(original[name]); (user_templates / name).write_bytes(original[name])
        (templates / name).write_bytes(plistlib.dumps({**config, 'UserName': 'leon', 'GroupName': 'staff'}))
    account = SimpleNamespace(pw_uid=501, pw_gid=20, pw_dir=str(home))
    calls = []
    loaded = {'gui/501/' + config['Label'] for config in configs.values()}
    def launch(*args, **kwargs):
        calls.append(args)
        if args[0] == 'print':
            return SimpleNamespace(returncode=0 if args[1] in loaded else 113, stdout='pid = 42\n')
        if args[0] == 'bootout':loaded.discard(args[1])
        if args[0] == 'bootstrap' and args[1] == 'system' and args[2].endswith('score-worker.plist'):
            raise RuntimeError('Simulated second bootstrap failure')
        return SimpleNamespace(returncode=0, stdout='')
    monkeypatch.setattr(jobs, 'SYSTEM_DAEMONS', daemons)
    monkeypatch.setattr(jobs.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(jobs.os, 'chown', lambda *a: None)
    monkeypatch.setattr(jobs, 'wait_exit', lambda pids: None)
    monkeypatch.setattr(jobs, 'launch', launch)
    with pytest.raises(RuntimeError, match='Simulated second'):
        jobs.install_system(root, 'leon', configs, account)
    assert not list(daemons.glob('*.plist'))
    for name, data in original.items():
        assert (agents / name).read_bytes() == data
        assert ('bootstrap', 'gui/501', str(agents / name)) in calls
