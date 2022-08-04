from pydoc import doc
import dataflows as DF

def expand_redirects():
    def func(rows):
        for row in rows:
            yield row
            doc_id = row['doc_id']
            kind, muni_code, code, year = doc_id.split('/')
            year = int(year)
            for h in row['history']:
                if h['year'] != year:
                    new_doc_id = '/'.join([kind, muni_code, code, str(h['year'])])
                    yield dict(
                        code=code,
                        muni_code=muni_code,
                        title=h['title'],
                        year=h['year'],
                        doc_id=new_doc_id,
                        __redirect=doc_id
                    )
            if len(code) < 4:
                new_doc_id = '/'.join([kind, muni_code, code])
                yield dict(
                    code=code,
                    muni_code=muni_code,
                    title=h['title'],
                    year=h['year'],
                    doc_id=new_doc_id,
                    __redirect=doc_id
                )
    return func


def flow(*_):
    return DF.Flow(
        DF.add_field('__redirect', 'string'),
        expand_redirects(),   
    )