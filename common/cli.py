from datapackage_pipelines.cli import cli
import os


def main(*args, **kwargs):
    os.environ.setdefault("DPP_PROCESSOR_PATH", os.path.join(os.path.dirname(__file__), '..', 'pipelines'))
    return cli(*args, **kwargs)
