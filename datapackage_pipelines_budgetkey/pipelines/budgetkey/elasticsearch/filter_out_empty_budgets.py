from datapackage_pipelines.wrapper import process

def flt(x):
    if x is None:
        return 0
    return float(x)


def process_row(row, *_):
    fields = ['net_allocated', 'net_revised', 'net_executed']
    s = sum(flt(row.get(f, 0))**2 for f in fields)
    s += sum(
        sum(flt(h.get(f, 0))**2 for f in fields)
        for h in row.get('history', {}).values()
    )
    if s > 0:
        return row


process(process_row=process_row)