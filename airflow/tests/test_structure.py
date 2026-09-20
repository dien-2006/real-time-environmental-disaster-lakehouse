"""Verify discovery without installing Airflow or contacting external services."""
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

DAGS = Path(__file__).resolve().parents[1] / 'dags'


class DagStructureTests(unittest.TestCase):
    def test_discovery_registers_sources_without_running_tasks(self):
        dags, tasks = [], []

        def dag(**options):
            def decorate(fn):
                def build():
                    dags.append(options)
                    fn()
                    return options
                return build
            return decorate

        def task(**options):
            def decorate(fn):
                tasks.append((options, fn))
                return lambda: None
            return decorate

        sdk = types.ModuleType('airflow.sdk')
        sdk.dag, sdk.task = dag, task
        with patch.dict(sys.modules, {'airflow.sdk': sdk}):
            with patch.object(sys, 'path', [str(DAGS), *sys.path]):
                result = runpy.run_path(str(DAGS / 'api_kafka_ingestion.py'))
                self.assertEqual(len(dags), 4)
                self.assertEqual(len({d['dag_id'] for d in dags}), 4)
                self.assertEqual(len(tasks), 4)
                self.assertNotIn('environment_ingestion.runner', sys.modules)
                for source, (_, _, schedule) in result['SOURCES'].items():
                    self.assertEqual(result[f'api_kafka_{source}']['schedule'], schedule)
                for options in dags:
                    self.assertEqual(options['max_active_runs'], 1)
                    self.assertFalse(options['catchup'])
                runner = types.ModuleType('environment_ingestion.runner')
                calls = []
                runner.run_source_once = calls.append
                with patch.dict(sys.modules, {'environment_ingestion.runner': runner}):
                    for options, fn in tasks:
                        self.assertEqual(options['retries'], 3)
                        self.assertFalse(options['do_xcom_push'])
                        fn()
                self.assertEqual(calls, list(result['SOURCES']))


if __name__ == '__main__':
    unittest.main()
