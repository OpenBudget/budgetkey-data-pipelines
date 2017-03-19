import os


def main(*args, **kwargs):
    original_directory = os.getcwd()
    try:
        base_path = os.path.join(os.path.dirname(__file__))
        os.chdir(os.path.join(base_path, 'pipelines'))
        os.environ.setdefault("DPP_PROCESSOR_PATH", os.path.join(base_path, 'processors'))
        # so we have to import after setting DPP_PROCESSOR_PATH because this environment variable is read on import
        from datapackage_pipelines.cli import cli
        cli(*args, **kwargs)
    finally:
        os.chdir(original_directory)
