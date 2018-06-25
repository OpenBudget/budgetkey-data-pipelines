from datapackage_pipelines.wrapper import spew, ingest


def process_resource(rows):
    for row in rows:
        founders = row.get('association_founders')
        registration_date = row.get('association_registration_date')
        association_name = row.get('association_title')
        id = row['id']
        if founders and registration_date:
            for founder in founders:
                rec = dict(
                    company=association_name,
                    full_name=founder,
                    event='founder',
                    title='{} מן המייסדים של {}'.format(founder, association_name),
                    when=registration_date,
                    sources=[
                        dict(
                            source='guidestar',
                            proof_url='https://www.guidestar.org.il/he/organization/{}/people'.format(id),
                            doc_id='/org/association/{}'.format(id)
                        )
                    ]
                )
                yield rec


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'] = [
        {'name': 'company', 'type': 'string'},
        {'name': 'full_name', 'type': 'string'},
        {'name': 'sources', 'type': 'array'},
        {'name': 'event', 'type': 'string'},
        {'name': 'title', 'type': 'string'},
        {'name': 'when', 'type': 'date'},
    ]
    return dp


if __name__ == '__main__':
    params, dp, res_iter = ingest()
    dp = modify_datapackage(dp)
    spew(dp, [process_resource(next(res_iter))])