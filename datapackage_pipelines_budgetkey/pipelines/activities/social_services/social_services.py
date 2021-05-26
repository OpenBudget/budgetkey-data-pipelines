import dataflows as DF

def datarecords(kind):
    return map(
        lambda r: r['value'],
        DF.Flow(
            DF.load(f'https://data-input.obudget.org/api/datarecords/{kind}', format='json', property='result')
        ).results()[0][0]
    )

def splitter(field_name):
    codelist = datarecords(field_name)
    codelist = dict((x.pop('id'), x.pop('name')) for x in codelist)
    print(field_name, codelist)
    def func(row):
        row[field_name] = [codelist[i] for i in row[field_name]]
    return DF.Flow(
        func,
        DF.set_type(field_name, **{'es:keyword': True})
    )
#  beneficiaries budgetItems complete description id intervention manualBudget name office subject subsubunit subunit suppliers target_age_group target_audience tenders unit virtue_of_table

def flow(*_):
    return DF.Flow(
        datarecords('social_service'),
        DF.delete_fields(['__tab', 'complete', 'non_suppliers', 'non_tenders', 'notes', ]),
        DF.add_field('publisher_name', 'string', lambda r: r['office'], **{'es:keyword': True}),
        splitter('target_audience'),
        splitter('subject'),
        splitter('intervention'),
        DF.add_field('min_year', 'integer', 2020),
        DF.add_field('max_year', 'integer', 2020),
        DF.add_field('kind', 'string', 'gov_social_service'),
        DF.add_field('kind_he', 'string', 'שירות חברתי'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer(),
    ).process()
