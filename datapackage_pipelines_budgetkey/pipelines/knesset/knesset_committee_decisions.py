import requests
import shutil
import os
import dataflows as DF

OUTPUT_PATH = '/var/datapackages/knesset/knesset_committee_decisions'

def flow(*_):
    out = []
    downloaded = 0
    committees = requests.get('https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=CommitteeTypeID%20eq%2070&$orderby=Name').json()
    committees = [
        dict(
            id=x['Id'],
            knesset_num=x['KnessetNum'],
        )
        for x in committees['value']
    ]
    print(f'GOT {len(committees)} committees')
    documents = requests.get(f'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_DocumentCommitteeSession?$filter=GroupTypeID%20eq%20106&$orderby=Id').json()
    print(f'GOT {len(documents["value"])} documents')
    for committee in committees:
        sessions = requests.get(f'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_CommitteeSession?$filter=CommitteeID%20eq%20{committee["id"]}&$orderby=Id').json()
        print(f'GOT {len(sessions["value"])} sessions for committee {committee["id"]}')
        for session in sessions['value']:
            sessionID = session['Id']
            # documents = requests.get(f'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_DocumentCommitteeSession?$filter=GroupTypeID%20eq%20106&$orderby=Id').json()
            for document in documents['value']:
                if document['ApplicationDesc'].lower() == 'pdf' and document['CommitteeSessionID'] == sessionID:
                    doc = dict(
                        url=document['FilePath'],
                        filename=f'{document["Id"]}.pdf',
                        date=document['LastUpdatedDate']
                    )
                    out.append(doc)
                    outpath = os.path.join(OUTPUT_PATH, doc['filename'])
                    if not os.path.exists(outpath):
                        document = requests.get(doc['url'], stream=True)
                        with open(outpath, 'wb') as o:
                            shutil.copyfileobj(document.raw, o)
                        downloaded += 1

        print(f'DOWNLOADED {downloaded} out of {len(out)} total documents (now in committee {committee["id"]})')

    return DF.Flow(
        out,
        DF.update_resource(-1, name='knesset_committee_decisions', path='index.csv', **{'dpp:streaming': True}),
        DF.dump_to_path(OUTPUT_PATH),
    )


if __name__ == "__main__":
    flow()