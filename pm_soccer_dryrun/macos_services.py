"""Prepare and supervise the complete local recording stack with launchd.

install-user starts login-scoped jobs for validation. install-system requires
sudo and migrates only these jobs to system LaunchDaemons, running as the user.
It never changes power settings, contacts the VPS, or deletes recordings.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import plistlib
import pwd
import re
import shutil
import subprocess
import time

ROLES = ('primary', 'score-worker', 'fotmob-receiver', 'fotmob-worker', 'resources')
SYSTEM_DAEMONS = Path('/Library/LaunchDaemons')


def configs(root, user, run_id):
    root = Path(root).resolve()
    python, app = root / '.venv/bin/python', root / 'app'
    primary, shadow = root / 'recordings/primary', root / 'recordings/fotmob'
    commands = {
        'primary': ['pm_soccer_dryrun', 'live', '--data-dir', str(primary), '--run-id', run_id,
                    '--book-window-minutes', '5', '--score-relay-bind', '127.0.0.1',
                    '--score-relay-peer', '127.0.0.1', '--score-relay-port', '18765',
                    '--fotmob-shadow-dir', str(shadow)],
        'score-worker': ['pm_soccer_dryrun.relay', '--vps', '127.0.0.1', '--port', '18765', '--name', 'm1max'],
        'fotmob-receiver': ['pm_soccer_dryrun.fotmob_shadow', 'receiver', '--primary-dir', str(primary), '--data-dir', str(shadow)],
        'fotmob-worker': ['pm_soccer_dryrun.fotmob_shadow', 'worker'],
        'resources': ['pm_soccer_dryrun.resource_sample_macos', '--config', str(root / 'resource-config.json')],
    }
    result = {}
    for role in ROLES:
        label = 'com.' + user + '.pm-soccer-' + role
        config = {
            'Label': label, 'WorkingDirectory': str(app),
            'ProgramArguments': [str(python), '-u', '-m', *commands[role]],
            'EnvironmentVariables': {'PYTHONUNBUFFERED': '1', 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONNOUSERSITE': '1'},
            'RunAtLoad': True, 'ThrottleInterval': 10 if role.startswith('fotmob') else 5,
            'ExitTimeOut': 60 if role == 'primary' else 30,
            'StandardOutPath': str(root / 'logs' / (role + '.stdout.log')),
            'StandardErrorPath': str(root / 'logs' / (role + '.stderr.log')),
        }
        if role == 'resources':
            config.update(StartInterval=60, ProcessType='Background', Nice=10)
        else:
            config['KeepAlive'] = True
        if role == 'fotmob-receiver':
            config['Nice'] = 5
        result[role] = config
    return result


def launch(*args, check=True):
    return subprocess.run(['/bin/launchctl', *args], check=check, capture_output=True, text=True, timeout=90)


def pid_for(domain, label):
    response = launch('print', domain + '/' + label, check=False)
    match = re.search(r'^\s*pid = (\d+)', response.stdout, re.M)
    return int(match[1]) if response.returncode == 0 and match else None


def wait_exit(pids):
    deadline = time.monotonic() + 85
    for pid in pids:
        while pid:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('Old local job did not stop: ' + str(pid))
            time.sleep(.2)


def prepare(root, user, run_id):
    account = pwd.getpwnam(user)
    assert root.is_dir() and (root / '.venv/bin/python').is_file()
    result = configs(root, user, run_id)
    for name in ('logs', 'recordings/primary', 'recordings/fotmob', 'launchagents', 'launchdaemons'):
        (root / name).mkdir(parents=True, exist_ok=True)
    for config in result.values():
        label = config['Label']
        (root / 'launchagents' / (label + '.plist')).write_bytes(plistlib.dumps(config))
        daemon = {**config, 'UserName': user, 'GroupName': 'staff'}
        (root / 'launchdaemons' / (label + '.plist')).write_bytes(plistlib.dumps(daemon))
    targets = [{'label': result[role]['Label'], 'role': role,
                'data_dir': str(root / ('recordings/fotmob' if role.startswith('fotmob') else 'recordings/primary'))}
               for role in ROLES if role != 'resources']
    (root / 'resource-config.json').write_text(json.dumps({'uid': account.pw_uid, 'targets': targets}, indent=2) + '\n')
    return result, account


def install_user(root, user, result, account):
    assert os.getuid() == account.pw_uid, 'Run install-user as the intended service user'
    domain = 'gui/' + str(account.pw_uid)
    launch('print', domain)
    agents = Path(account.pw_dir) / 'Library/LaunchAgents'
    agents.mkdir(parents=True, exist_ok=True)
    installed = []
    try:
        for role, config in result.items():
            label = config['Label']
            assert launch('print', 'system/' + label, check=False).returncode != 0, 'System service already loaded'
            source, target = root / 'launchagents' / (label + '.plist'), agents / (label + '.plist')
            if target.exists():
                assert target.read_bytes() == source.read_bytes(), 'Existing unrelated/different job: ' + str(target)
            else:
                shutil.copyfile(source, target)
            if launch('print', domain + '/' + label, check=False).returncode == 0:
                continue
            launch('enable', domain + '/' + label)
            launch('bootstrap', domain, str(target))
            installed.append((label, target))
    except BaseException:
        for label, target in reversed(installed):
            launch('bootout', domain + '/' + label, check=False)
        raise
    print('Started local LaunchAgents. System migration still requires the supplied sudo installer.')


def install_system(root, user, result, account):
    if os.geteuid() != 0:
        raise PermissionError('System LaunchDaemons require sudo; no jobs were migrated.')
    domain = 'gui/' + str(account.pw_uid)
    agents = Path(account.pw_dir) / 'Library/LaunchAgents'
    daemons = SYSTEM_DAEMONS
    # Validate every source and destination before changing any running job.
    for config in result.values():
        label = config['Label']
        source = root / 'launchdaemons' / (label + '.plist')
        daemon = plistlib.loads(source.read_bytes())
        assert daemon['UserName'] == user and daemon['GroupName'] == 'staff'
        target = daemons / source.name
        assert not target.exists(), 'System job already installed; inspect before rerunning: ' + label
        assert launch('print', 'system/' + label, check=False).returncode != 0
        agent = agents / source.name
        if agent.exists():
            assert agent.read_bytes() == (root / 'launchagents' / agent.name).read_bytes()
    backup = root / ('agent-migration-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    backup.mkdir()
    previous, created = {}, []
    for config in result.values():
        label = config['Label']
        agent = agents / (label + '.plist')
        previous[label] = {'loaded': launch('print', domain + '/' + label, check=False).returncode == 0,
                           'pid': pid_for(domain, label), 'file': agent.exists()}
        if agent.exists():
            shutil.copy2(agent, backup / agent.name)
    try:
        for config in reversed(list(result.values())):
            label = config['Label']
            if previous[label]['loaded']:
                launch('bootout', domain + '/' + label)
            agent = agents / (label + '.plist')
            if agent.exists():
                agent.unlink()
        wait_exit([state['pid'] for state in previous.values() if state['pid']])
        for config in result.values():
            label = config['Label']
            target = daemons / (label + '.plist')
            shutil.copyfile(root / 'launchdaemons' / target.name, target)
            os.chown(target, 0, 0)
            target.chmod(0o644)
            created.append(target)
            launch('enable', 'system/' + label)
            launch('bootstrap', 'system', str(target))
    except BaseException:
        for target in reversed(created):
            launch('bootout', 'system/' + target.stem, check=False)
            target.unlink(missing_ok=True)
        for label, state in previous.items():
            agent = agents / (label + '.plist')
            if state['file']:
                shutil.copy2(backup / agent.name, agent)
                os.chown(agent, account.pw_uid, account.pw_gid)
            if state['loaded'] and launch('print', domain + '/' + label, check=False).returncode != 0:
                launch('bootstrap', domain, str(agent), check=False)
        raise
    receipt_path = root / 'deployment.json'
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
        receipt.update(supervisor_domain='system', system_daemon_install_pending=False,
                       system_migration_utc=datetime.now(timezone.utc).isoformat())
        receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    print('Installed five system LaunchDaemons running as ' + user + '. Desktop login is no longer required.')
    print('Primary restarts into a fresh session; previous local recordings are preserved. VPS/Genie are untouched.')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'install-user', 'install-system'))
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    assert re.fullmatch(r'[a-z_][a-z0-9_-]*', args.user)
    assert re.fullmatch(r'[A-Za-z0-9_-]+', args.run_id)
    if args.mode == 'install-system':
        # Do not rewrite user-owned application configuration as root.
        result, account = configs(root, args.user, args.run_id), pwd.getpwnam(args.user)
        for role, config in result.items():
            expected = {**config, 'UserName': args.user, 'GroupName': 'staff'}
            assert plistlib.loads((root / 'launchdaemons' / (config['Label'] + '.plist')).read_bytes()) == expected
        install_system(root, args.user, result, account)
    else:
        result, account = prepare(root, args.user, args.run_id)
        if args.mode == 'install-user':
            install_user(root, args.user, result, account)
        else:
            print('Validated and prepared five launchd jobs in ' + str(root))


if __name__ == '__main__':
    main()
