from datapackage_pipelines.wrapper import process


def remove_formulas_value(v):
    if isinstance(v, str):
        if v.startswith('="') and v.endswith('"'):
            v = v[2:-1]
    return v


def remove_formulas(row, *args):
    return dict(
        (k, remove_formulas_value(v))
        for k, v in row.items()
    )

process(process_row=remove_formulas)