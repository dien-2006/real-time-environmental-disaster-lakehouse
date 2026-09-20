import os
from pathlib import Path


def build_environment(project_root, base=None):
    env = dict(os.environ if base is None else base)
    root = Path(project_root).resolve()
    env.setdefault('AIRFLOW_HOME', str(root / '.state' / 'airflow'))
    env.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(root / 'airflow' / 'dags'))
    env.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'false')
    env.setdefault('AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX', 'glob')
    env.setdefault('AIRFLOW__CORE__PARALLELISM', '4')
    env.setdefault('AIRFLOW__API__PORT', '8081')
    env.setdefault('AIRFLOW__API__BASE_URL', 'http://localhost:8081')
    env.setdefault('AIRFLOW__CORE__EXECUTION_API_SERVER_URL', 'http://localhost:8081/execution/')
    env.setdefault('API_KAFKA_STATE_DIR', str(root / '.state' / 'airflow-ingestion'))
    env['PYTHONPATH'] = os.pathsep.join(filter(None, [
        str(root), str(root / 'airflow' / 'dags'), env.get('PYTHONPATH')]))
    return env
