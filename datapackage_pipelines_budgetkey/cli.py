import os
from datapackage_pipelines.cli import cli


def main(*args, **kwargs):
    original_directory = os.getcwd()
    try:
        base_path = os.path.join(os.path.dirname(__file__))
        os.chdir(os.path.join(base_path, 'pipelines'))
        os.environ.setdefault("DPP_PROCESSOR_PATH", os.path.join(base_path, 'processors'))
        os.environ.setdefault("DPP_DB_ENGINE", "sqlite+pysqlite:///{}".format(os.path.join(base_path, ".data.db")))
        cli(*args, **kwargs)
    finally:
        os.chdir(original_directory)
