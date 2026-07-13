import requests
import shutil
import os
import dataflows as DF

OUTPUT_PATH = '/var/datapackages/knesset/knesset_committee_decisions'
os.makedirs(OUTPUT_PATH, exist_ok=True)


def _odata_paged(url):
    """Iterate every record from an OData v4 endpoint.

    Knesset's OData v4 server returns at most 100 rows per response and
    advertises the next page in `@odata.nextLink`. The previous version of
    this pipeline ignored that link and consumed only the first 100 rows
    of the global `KNS_DocumentCommitteeSession` query — which, ordered by
    `Id`, are all from Knesset 18/20 (2016). The result was an `index.csv`
    that froze in 2016 even though the live OData store now has 12.5M+
    matching documents.
    """
    while url:
        resp = requests.get(url).json()
        for item in resp.get('value', []):
            yield item
        url = resp.get('@odata.nextLink')


def flow(*_):
    out = []
    downloaded = 0
    committees = [
        dict(id=x['Id'], knesset_num=x['KnessetNum'])
        for x in _odata_paged(
            'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee'
            '?$filter=CommitteeTypeID%20eq%2070&$orderby=Name'
        )
    ]
    print(f'GOT {len(committees)} committees')

    for committee in committees:
        sessions = list(_odata_paged(
            'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_CommitteeSession'
            f'?$filter=CommitteeID%20eq%20{committee["id"]}&$orderby=Id'
        ))
        print(f'GOT {len(sessions)} sessions for committee {committee["id"]}')
        for session in sessions:
            session_id = session['Id']
            # Per-session, paginated document fetch. Pushing the
            # `CommitteeSessionID eq …` predicate into OData keeps each
            # response small (typically 0–3 PDFs per session) and lets us
            # walk the entire history correctly.
            documents = _odata_paged(
                'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_DocumentCommitteeSession'
                f'?$filter=CommitteeSessionID%20eq%20{session_id}'
                '%20and%20GroupTypeID%20eq%20106&$orderby=Id'
            )
            for document in documents:
                if document.get('ApplicationDesc', '').lower() != 'pdf':
                    continue
                doc = dict(
                    url=document['FilePath'],
                    filename=f'{document["Id"]}.pdf',
                    date=document['LastUpdatedDate'],
                    knesset_num=committee['knesset_num'],
                )
                out.append(doc)
                outpath = os.path.join(OUTPUT_PATH, doc['filename'])
                if not os.path.exists(outpath):
                    pdf_resp = requests.get(doc['url'], stream=True)
                    with open(outpath, 'wb') as o:
                        shutil.copyfileobj(pdf_resp.raw, o)
                    downloaded += 1

        print(f'DOWNLOADED {downloaded} out of {len(out)} total documents (now in committee {committee["id"]})')

    return DF.Flow(
        out,
        DF.update_resource(-1, name='knesset_committee_decisions', path='index.csv', **{'dpp:streaming': True}),
        DF.dump_to_path(OUTPUT_PATH),
    )


if __name__ == "__main__":
    flow()
