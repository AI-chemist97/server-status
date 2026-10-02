"""Collect server metrics and publish them as data/server.json to a git repository.

Changelog:
- 0.0.0.2026.10.2: initial release (Prefect flow, once/serve modes, git publish)
- 0.1.0.2026.10.2: report the state of explicitly named docker containers (--containers)
- 0.2.0.2026.10.2: --containers takes label=container pairs; only the label is published
"""
__author__ = 'yRocket'
__version__ = "0.2.0.2026.10.2"  # Semantic Versioning: Major.Minor.Patch.Date(YYYY.M.D)
__all__ = ['Metrics', 'RunMode', 'read_container_states', 'collect_metrics', 'write_metrics', 'publish_metrics',
           'status_flow']

import argparse
import json
import math
import pathlib
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import Literal

import psutil
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

CPU_SAMPLE_SECONDS: float = 1.0
SECONDS_PER_HOUR: float = 3600.0
SECONDS_PER_MINUTE: int = 60
DATA_FILE: pathlib.Path = pathlib.Path('data') / 'server.json'
FLOW_NAME: str = 'server-status'
GIT_TIMEOUT_SECONDS: int = 60
DOCKER_TIMEOUT_SECONDS: int = 30
DOCKER_MISSING_MARKER: str = 'No such object'
CONTAINER_MISSING: str = 'missing'


class RunMode(StrEnum):
    ONCE = auto()
    SERVE = auto()


@dataclass(frozen=True)
class Metrics:
    """One snapshot of the server. Percent fields are in [0, 100]; no host identifier is ever stored.

    containers maps each requested public label to the docker state of its container (running, exited, missing, ...).
    The real container name is never stored.
    """
    updated: str
    cpu_pct: float
    mem_pct: float
    disk_pct: float
    uptime_h: float
    containers: dict = field(default_factory=dict)

    def __post_init__(self):
        for name, state in self.containers.items():
            if not name.strip() or not state.strip():
                raise ValueError(f"empty container name or state: {name!r}={state!r}")
        for name in ('cpu_pct', 'mem_pct', 'disk_pct'):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0.0 <= value <= 100.0:
                raise ValueError(f"{name} out of range: {value!r}")
        if not math.isfinite(self.uptime_h) or self.uptime_h < 0:
            raise ValueError(f"uptime_h invalid: {self.uptime_h!r}")


def read_container_states(containers: dict) -> dict:
    """Ask docker for the state of each requested container.

    Args:
        containers: Mapping public label -> real container name. Nothing outside this mapping is read or published.

    Returns:
        Mapping label -> docker state (running, exited, restarting, ...), or 'missing' when no such container exists.
        Any other docker failure (daemon down, permission denied) is raised, not reported as a state.
    """
    states = {}
    for label, name in containers.items():
        result = subprocess.run(['docker', 'container', 'inspect', '--format', '{{.State.Status}}', name],
                                capture_output=True, text=True, timeout=DOCKER_TIMEOUT_SECONDS)
        if result.returncode == 0:
            state = result.stdout.strip()
            if not state:
                raise ValueError(f"docker returned an empty state for container {name!r}")
            states[label] = state
        elif DOCKER_MISSING_MARKER in result.stderr:
            states[label] = CONTAINER_MISSING
        else:
            raise RuntimeError(f"docker inspect {name!r} failed ({result.returncode}): {result.stderr.strip()}")
    return states


@task(cache_policy=NO_CACHE)
def collect_metrics(disk_path: str, containers: dict) -> Metrics:
    """Sample CPU, memory, disk, uptime and the requested containers of this machine.

    Args:
        disk_path: A path on the filesystem whose usage is reported.
        containers: Mapping public label -> real container name whose docker state is reported; empty means none.

    Returns:
        A validated Metrics snapshot stamped with the current UTC time.
    """
    return Metrics(
        containers=read_container_states(containers=containers),
        updated=datetime.now(timezone.utc).isoformat(timespec='seconds'),
        cpu_pct=psutil.cpu_percent(interval=CPU_SAMPLE_SECONDS),
        mem_pct=psutil.virtual_memory().percent,
        disk_pct=psutil.disk_usage(disk_path).percent,
        uptime_h=round((time.time() - psutil.boot_time()) / SECONDS_PER_HOUR, 2),
    )


@task(cache_policy=NO_CACHE)
def write_metrics(metrics: Metrics, data_file: pathlib.Path) -> None:
    """Write metrics to data_file atomically, so a reader never sees a half-written file."""
    data_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = data_file.with_name(data_file.name + '.tmp')
    tmp_file.write_text(json.dumps(asdict(metrics), indent=2) + '\n', encoding='utf-8')
    tmp_file.replace(data_file)


def run_git(repo_folder: pathlib.Path, *git_args: str) -> None:
    """Run a git command in repo_folder; any failure is raised with its stderr, never swallowed."""
    result = subprocess.run(['git', '-C', str(repo_folder), *git_args], capture_output=True, text=True,
                            timeout=GIT_TIMEOUT_SECONDS)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(git_args)} failed ({result.returncode}): {result.stderr.strip()}")


@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=30)
def publish_metrics(repo_folder: pathlib.Path, data_file: pathlib.Path, branch: str, updated: str) -> None:
    """Commit data_file and push it to origin/<branch>."""
    run_git(repo_folder, 'add', str(data_file))
    run_git(repo_folder, 'commit', '-m', f"status: {updated}")
    run_git(repo_folder, 'push', 'origin', branch)


@flow(name=FLOW_NAME)
def status_flow(output_folder: str, disk_path: str, containers: dict, branch: str, push: bool) -> None:
    """Collect one snapshot, write it under output_folder, and optionally push it."""
    repo_folder = pathlib.Path(output_folder)
    data_file = repo_folder / DATA_FILE
    metrics = collect_metrics(disk_path=disk_path, containers=containers)
    write_metrics(metrics=metrics, data_file=data_file)
    if push:
        publish_metrics(repo_folder=repo_folder, data_file=data_file, branch=branch, updated=metrics.updated)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=pathlib.Path(__file__).name,
        usage=f"%(prog)s {__version__}\n       [-h] [-v] --output-folder FOLDER --run {{once,serve}} "
              f"[--interval-minutes N] [--disk-path PATH] [--containers LABEL=NAME,...] [--branch NAME] "
              f"[--push {{true,false}}]",
        description="Collect CPU/memory/disk/uptime and publish them as data/server.json to a git repository.",
    )
    parser.add_argument('-v', '--version', action='version', version=f"%(prog)s {__version__}")
    parser.add_argument('--output-folder', type=pathlib.Path, help="Clone of the status repository (output root).")
    parser.add_argument('--run', choices=[mode.value for mode in RunMode],
                        help="once: collect a single snapshot; serve: keep collecting on a schedule.")
    parser.add_argument('--interval-minutes', type=int, default=None,
                        help="Minutes between snapshots. Required with --run serve, rejected with --run once.")
    parser.add_argument('--disk-path', default='/', help="Path whose disk usage is reported (default: /).")
    parser.add_argument('--containers', default=None,
                        help="Comma-separated label=container pairs, e.g. isa=autotrade-isa. Only the label and "
                             "the state are published; omit to report none.")
    parser.add_argument('--branch', default='main', help="Branch to push (default: main).")
    parser.add_argument('--push', choices=['true', 'false'], default='true',
                        help="Commit and push each snapshot (default: true).")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    args = parser.parse_args()

    if args.output_folder is None or args.run is None:
        parser.error("--output-folder and --run are required.")
    if not args.output_folder.is_dir():
        parser.print_help()
        parser.error(f"--output-folder is not a folder: {args.output_folder}")
    args.push = args.push == 'true'
    pairs = [] if args.containers is None else [item.partition('=') for item in args.containers.split(',')]
    args.container_map = {label.strip(): name.strip() for label, sep, name in pairs}
    if any(sep != '=' or not label.strip() or not name.strip() for label, sep, name in pairs) \
            or len(args.container_map) != len(pairs):
        parser.error(f"--containers needs distinct, non-empty label=container pairs: {args.containers!r}")
    if args.container_map and shutil.which('docker') is None:
        parser.error("--containers needs the docker CLI on PATH, but it was not found.")
    if args.push and not (args.output_folder / '.git').exists():
        parser.error(f"--push true needs a git clone, but {args.output_folder} has no .git")
    if args.run == RunMode.SERVE and (args.interval_minutes is None or args.interval_minutes < 1):
        parser.error("--run serve needs --interval-minutes >= 1.")
    if args.run == RunMode.ONCE and args.interval_minutes is not None:
        parser.error("--interval-minutes only applies to --run serve; pass one or the other.")
    return args


if __name__ == '__main__':
    args = parse_args()
    flow_parameters = dict(output_folder=str(args.output_folder), disk_path=args.disk_path,
                           containers=args.container_map, branch=args.branch, push=args.push)
    run_mode: Literal['once', 'serve'] = args.run
    if run_mode == RunMode.ONCE:
        status_flow(**flow_parameters)
    else:
        status_flow.serve(name=f"{FLOW_NAME}-collector", interval=args.interval_minutes * SECONDS_PER_MINUTE,
                          parameters=flow_parameters)
