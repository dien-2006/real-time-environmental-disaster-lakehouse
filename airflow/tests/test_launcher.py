import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from environment_ingestion.cli import commands_for, main
from environment_ingestion.runtime import build_environment


class LauncherTests(unittest.TestCase):
    def test_run_enables_before_trigger_and_deduplicates_sources(self):
        self.assertEqual(commands_for('run', ['usgs', 'usgs']), [
            ['dags', 'unpause', 'api_kafka_usgs'],
            ['dags', 'trigger', 'api_kafka_usgs']])

    def test_runtime_preserves_deployment_overrides(self):
        env = build_environment('/tmp/project', {'API_KAFKA_STATE_DIR': '/data/state',
                                                'AIRFLOW_HOME': '/data/airflow'})
        self.assertEqual(env['API_KAFKA_STATE_DIR'], '/data/state')
        self.assertEqual(env['AIRFLOW_HOME'], '/data/airflow')
        self.assertIn('/tmp/project/airflow/dags', env['PYTHONPATH'])

    def test_failure_stops_before_trigger(self):
        with patch('environment_ingestion.cli.importlib.metadata.version', return_value='3.1.8'):
            with patch('environment_ingestion.cli.subprocess.run') as run:
                run.return_value.returncode = 1
                self.assertEqual(main('/tmp', ['run']), 1)
                self.assertEqual(run.call_count, 1)

    def test_dry_run_does_not_launch_airflow(self):
        with patch('environment_ingestion.cli.subprocess.run') as run:
            self.assertEqual(main('/tmp', ['run', '--dry-run']), 0)
            run.assert_not_called()
