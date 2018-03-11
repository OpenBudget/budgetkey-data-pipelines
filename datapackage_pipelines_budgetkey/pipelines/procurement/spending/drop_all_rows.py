from datapackage_pipelines.wrapper import process


def process_row(*_):
    return None

process(process_row=process_row)