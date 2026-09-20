"""Entry point for the Airflow-managed pipeline; main.py remains independent."""
from pathlib import Path
import sys


def main():
    project_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(project_root / 'airflow' / 'dags'))
    from environment_ingestion.cli import main as airflow_main
    return airflow_main(project_root)


if __name__ == '__main__':
    raise SystemExit(main())
