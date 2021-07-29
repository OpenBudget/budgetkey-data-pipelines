import dataflows as DF

def flow(*_):
    return DF.Flow(
        DF.load('endowments.xlsx'),
        DF.set_type('id', type='string'),
        DF.set_type('registration_year', type='integer'),
        DF.set_type('endower', type='string'),
        DF.set_type('areas_of_activity', type='array', **{'es:type': 'string'}, transform=lambda x: x.split(',') if x else []),
        DF.set_type('areas_of_activity_secondary', type='array', **{'es:type': 'string'}, transform=lambda x: x.split(',') if x else []),
        DF.set_type('endowment_size', type='number'),
        DF.set_type('last_report_year', type='integer'),
        DF.set_type('management_method', type='string'),
        DF.set_type('kind_he', type='string'),
        DF.update_resource(-1, **{'name': 'endowments'}),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__ == '__main__':
    DF.Flow(flow(), DF.printer()).process()