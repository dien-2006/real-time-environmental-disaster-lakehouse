"""Management commands, separate from task execution and DAG discovery."""
import argparse
import importlib.metadata
import os
import subprocess
import sys

from environment_ingestion.runtime import build_environment
from environment_ingestion.sources import SOURCES


def commands_for(action, sources):
    if action == 'start':
        return [['standalone']]
    if action == 'check':
        return [['dags', 'list-import-errors']]
    commands = []
    for source in dict.fromkeys(sources):
        dag_id = 'environment_bronze_silver_gold' if source == 'transform' else f'api_kafka_{source}'
        if action == 'run':
            commands.extend([['dags', 'unpause', dag_id], ['dags', 'trigger', dag_id]])
        elif action == 'pause':
            commands.append(['dags', 'pause', dag_id])
        elif action == 'status':
            commands.append(['dags', 'list-runs', dag_id])
    return commands


def main(project_root, argv=None):
    parser = argparse.ArgumentParser(description='Manage the Airflow 3 API → Kafka pipeline')
    parser.add_argument('action', choices=['start', 'run', 'pause', 'status', 'check'])
    parser.add_argument('--sources', nargs='+', choices=[*SOURCES, 'transform'], default=['usgs'])
    parser.add_argument('--dry-run', action='store_true', help='Print commands without running Airflow')
    args = parser.parse_args(argv)
    commands = commands_for(args.action, args.sources)
    if args.dry_run:
        for command in commands:
            print('python -m airflow ' + ' '.join(command))
        return 0
    try:
        version = importlib.metadata.version('apache-airflow')
    except importlib.metadata.PackageNotFoundError:
        print('Airflow is not installed in this Python environment. See airflow/README.md.', file=sys.stderr)
        return 1
    if version.split('.')[0] != '3':
        print('This pipeline requires Airflow 3.', file=sys.stderr)
        return 1
    env = build_environment(project_root)
    if args.action == 'run':
        print('Enabling schedules and submitting a run. Use pause to stop future scheduling.', flush=True)
    if args.action == 'start':
        # Replace launcher so Ctrl+C and termination reach the standalone supervisor.
        os.execve(sys.executable, [sys.executable, '-m', 'airflow', 'standalone'], env)
    for command in commands:
        result = subprocess.run([sys.executable, '-m', 'airflow', *command],
                                cwd=project_root, env=env, check=False)
        if result.returncode:
            return result.returncode
    return 0
