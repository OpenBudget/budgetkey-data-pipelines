import dataflows as DF

def flow(*_):
    return DF.Flow(
        DF.load('partnerships.xlsx'),
        DF.set_type('id', type='string', transform=str),
        DF.set_type('name_en', type='string', transform=str),
        DF.set_type('partnership_registration_date', type='date', format='%d/%m/%Y'),
        DF.set_type('partnership_city', type='string'),
        DF.set_type('partnership_street', type='string', transform=str),
        DF.set_type('partnership_house_number', type='string', transform=str),
        DF.set_type('partnership_zipcode', type='string', transform=str),
        DF.set_type('partnership_pob', type='string', transform=str),
        DF.set_type('partnership_country', type='string'),
        DF.set_type('partnership_located_at', type='string', transform=str),
        DF.update_resource(-1, **{'name': 'partnerships'}),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__ == '__main__':
    DF.Flow(flow(), DF.printer()).process()