"""Migrate Genie's two soccer LaunchAgents to LaunchDaemons; requires sudo.

Use --check to validate without changing the running services.
Processes keep running as noel, not root. No FileVault/power settings change.
"""
import argparse
import datetime
import os
import pathlib
import plistlib
import re
import shutil
import subprocess
import time

SERVICE = pathlib.Path('/Users/noel/services/pm-soccer-score-worker')
AGENTS = pathlib.Path('/Users/noel/Library/LaunchAgents')
DAEMONS = pathlib.Path('/Library/LaunchDaemons')
LABELS = ['com.noel.pm-soccer-score-tunnel', 'com.noel.pm-soccer-score-worker']


def launch(*args, check=True):
    return subprocess.run(['/bin/launchctl', *args], check=check, capture_output=True, text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    templates = pathlib.Path(__file__).resolve().parent / 'launchdaemons'
    for label in LABELS:
        path = templates / (label + '.plist')
        config = plistlib.loads(path.read_bytes())
        assert config['Label'] == label and config['UserName'] == 'noel' and config['GroupName'] == 'staff'
        assert config['RunAtLoad'] and config['KeepAlive']
        assert pathlib.Path(config['ProgramArguments'][0]).exists()
        assert not (DAEMONS / path.name).exists(), 'System plist already exists; inspect before rerunning'
        assert launch('print', 'system/' + label, check=False).returncode != 0, 'System job already loaded'
        assert (AGENTS / path.name).exists(), 'Expected existing LaunchAgent is missing'
    assert (SERVICE / 'ssh_config').exists()
    assert pathlib.Path('/Users/noel/.ssh/pm_soccer_relay_ed25519').exists()
    if args.check:
        print('Validated: two system daemons running as noel; existing agents will be backed up and retired.')
        return
    if os.geteuid() != 0:
        parser.error('Run with sudo to install /Library/LaunchDaemons; no changes made.')
    backup = SERVICE / ('launchagent-backup-' + datetime.datetime.now().strftime('%Y%m%dT%H%M%S'))
    backup.mkdir(mode=0o700)
    previous = {}
    for label in LABELS:
        shutil.copy2(AGENTS / (label + '.plist'), backup / (label + '.plist'))
        old = launch('print', 'gui/501/' + label, check=False)
        pid = re.search(r'\n\s*pid = (\d+)', old.stdout)
        previous[label] = {'loaded': old.returncode == 0, 'pid': int(pid[1]) if pid else None}
    try:
        for label in reversed(LABELS):
            if previous[label]['loaded']:
                launch('bootout', 'gui/501/' + label)
            (AGENTS / (label + '.plist')).unlink()
        deadline = time.monotonic() + 50
        for state in previous.values():
            while state['pid']:
                try: os.kill(state['pid'], 0)
                except ProcessLookupError: break
                if time.monotonic() >= deadline: raise RuntimeError('Old service did not exit; rolling back')
                time.sleep(.2)
        for label in LABELS:
            target = DAEMONS / (label + '.plist')
            shutil.copyfile(templates / target.name, target)
            os.chown(target, 0, 0); target.chmod(0o644)
            launch('enable', 'system/' + label)
            launch('bootstrap', 'system', str(target))
            launch('print', 'system/' + label)
    except BaseException:
        for label in reversed(LABELS):
            launch('bootout', 'system/' + label, check=False)
            target = DAEMONS / (label + '.plist')
            if target.exists(): target.unlink()
        for label in LABELS:
            original = AGENTS / (label + '.plist')
            shutil.copy2(backup / original.name, original)
            os.chown(original, 501, 20)
            if previous[label]['loaded'] and launch('print', 'gui/501/' + label, check=False).returncode != 0:
                launch('bootstrap', 'gui/501', str(original), check=False)
        print('Migration failed; original agent files restored. Inspect launchctl status.')
        raise
    print('Installed both system LaunchDaemons, running as noel. They do not require a desktop login.')
    print('Previous LaunchAgents backed up to:', backup)
    print('FileVault is unchanged: disk unlock is still required after a cold boot.')
    print('Verify live score delivery on the VPS before logging out or rebooting.')


if __name__ == '__main__':
    main()
