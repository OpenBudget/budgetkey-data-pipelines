from datapackage_pipelines.wrapper import process

from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.check_existing import (tender_id_from_url,
                                                                                          publication_id_from_url)
from datapackage_pipelines.utilities.extended_json import json

from pyquery import PyQuery as pq
import magic
from datetime import datetime
import requests, os, base64, mimetypes, logging
from requests import HTTPError

url_prefix="https://www.mr.gov.il"


TABLE_SCHEMA = {
    "primaryKey": ["publication_id", "tender_type", "tender_id"],
    "fields": [
        {"name": "publisher_id", "type": "integer", "required": False},
        {"name": "publication_id", "type": "integer", "required": False, "description": "* exemptions: pId\n"
                                                                                        "* office: pId\n"
                                                                                        "* central: מספר המכרז במנוף"},
        {"name": "tender_type", "type": "string", "required": True},
        {"name": "page_url", "title": "page url", "type": "string", "format": "uri", "required": True},
        {"name": 'description', "type": "string", "required": True},
        {"name": "supplier_id", "type": "string", "required": False, "description": "* exemptions: ח.פ\n"
                                                                                    "* office: not used"},
        {"name": "supplier", "type": "string", "required": False, "description": "* exemptions: supplier company name\n"
                                                                                "* office: not used\n"
                                                                                 "* central: ספק זוכה"},
        {"name": "contact", "type": "string", "required": False, "description": "* exemptions: supplier contact name\n"
                                                                                "* office: not used\n"
                                                                                "* central: איש קשר"},
        {"name": "publisher", "type": "string", "required": False},
        {"name": "contact_email", "type": "string", "required": False, "description": "* exemptions: supplier contact email\n"
                                                                                      "* office: not used"},
        {"name": "claim_date", "type": "datetime", "required": False, "description": "* exemptions: תאריך אחרון להגשת השגות\n"
                                                                                     "* office: מועד אחרון להגשה"},
        {"name": "last_update_date", "type": "date", "required": False, "description": "* exemptions: תאריך עדכון אחרון\n"
                                                                                      "* office: תאריך עדכון אחרון"},
        {"name": "reason", "type": "string", "required": False, "description": "* exemptions: נימוקים לבקשת הפטור\n"
                                                                               "* office: not used"},
        {"name": "source_currency", "type": "string", "required": False},
        {"name": "regulation", "type": "string", "required": False, "description": "* exemptions: מתן הפטור מסתמך על תקנה\n"
                                                                                  "* office: not used\n"
                                                                                   "* central: סוג ההליך"},
        {"name": "volume", "type": "number", "required": False, "groupChar": ","},
        {"name": "subjects", "type": "string", "required": False, "description": "* exemptions: נושא/ים\n"
                                                                                "* office: נושא/ים\n"
                                                                                 "* central: נושא/ים"},
        {"name": "start_date", "type": "date", "required": False, "description": "* exemptions: תחילת תקופת התקשרות\n"
                                                                                "* office: תאריך פרסום"},
        {"name": "end_date", "type": "date", "required": False, "description": "* exemptions: תום תקופת ההתקשרות\n"
                                                                              "* office: not used\n"
                                                                               "* central: תאריך סיום תוקף מכרז"},
        {"name": "decision", "type": "string", "required": False, "description": "* exemptions: סטטוס החלטה\n"
                                                                                "* office: סטטוס\n"
                                                                                 "* central: סטטוס"},
        {"name": "page_title", "type": "string", "required": False, "description": "* exemptions: title of the exemption page\n"
                                                                                  "* office: not used"},
        {"name": "tender_id", "type": "string", "required": False, "description": "* exemptions: not used\n"
                                                                                  "* office: מספר המכרז"},
        {"name": "documents", "type": "array", "required": False}
    ]
}

BASE_URL = "https://www.mr.gov.il"


def parse_date(s):
    if s is None or s.strip() == '':
        return None
    try:
        return datetime.strptime(s, "%H:%M %d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")

def parse_datetime(s):
    if s is None or s.strip() == '':
        return None
    try:
        return datetime.strptime(s, "%H:%M %d/%m/%Y").strftime("%Y-%m-%dT%H:%M:0Z")
    except ValueError:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%dT%H:%M:0Z")


base_object_name = "procurement/tenders/"

def requests_get_content(url):
    return requests.get(url, timeout=60).content

def write_to_object_storage(object_name, data):
    logging.error('write_to_object_storage %s', object_name)
    if not object_storage.exists(object_name):
        ret = object_storage.write(object_name, data=data, public_bucket=True, create_bucket=True)
    else:
        ret = object_storage.urlfor(object_name)
    return ret

def unsign_document_link(url):
    url = url.replace("http://", "https://")
    if not url.startswith("https://www.mr.gov.il/Files_Michrazim/"):
        raise Exception("invalid url: {}".format(url))
    filename = url.replace("https://www.mr.gov.il/Files_Michrazim/", "").replace(".signed", "")
    decoded_indicator = base_object_name + filename + '.decoded'
    if object_storage.exists(decoded_indicator):
        decoded_indicator_url = object_storage.urlfor(decoded_indicator)
        ret = requests.get(decoded_indicator_url).text
        return ret
    try:
        content = requests_get_content(url)
        page = pq(content)
        data_elt = page(page(page.children()[1]).children()[0]).children()[0]
        assert b'The requested operation is not supported, and therefore can not be displayed' not in content
    except Exception as e:
        logging.error('Failed to download from %s (%s), returning original url', url, e)
        return url
    try:
        if data_elt.attrib["DataEncodingType"] != "base64":
            raise Exception("unknown DataEncodingType: {}".format(data_elt.attrib["DataEncodingType"]))
    except KeyError:
        return None
    buffer = data_elt.text
    if buffer:
        buffer = base64.decodebytes(buffer.encode("ascii"))
    else:
        buffer = ''
    mime = data_elt.attrib["MimeType"]
    guessed_mime = None
    orig_filename = None
    try:
        page.remove_namespaces()
        orig_filename = next(page[0].iterdescendants('FileName')).text
        _, ext = os.path.splitext(orig_filename)
    except:
        ext = mimetypes.guess_extension(mime, strict=False)
    if not ext:
        with magic.Magic(flags=magic.MAGIC_MIME_TYPE) as m:
            guessed_mime = m.id_buffer(buffer)
            logging.info('Attempted to detect buffer type: %s', guessed_mime)
            if guessed_mime == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                ext = '.docx'
            else:
                ext = mimetypes.guess_extension(guessed_mime)
    assert ext, "Unknown file type mime:%s filename:%s guessed_mime:%s ext:%r buffer:%r" % (mime, orig_filename, guessed_mime, ext, buffer[:128])
    object_name = base_object_name + filename + (ext if ext else "")
    ret = write_to_object_storage(object_name, buffer)
    write_to_object_storage(decoded_indicator, ret)
    return ret

def get_document_link(base_url, href):
    source_url = "{}{}".format(base_url, href)
    if source_url.endswith(".signed"):
        # signed documents - decrypt and change path to local unsigned file
        return unsign_document_link(source_url)
    else:
        # unsigned documents are returned with the source url as-is
        return source_url

def get_exemptions_data(row, page, documents):
    input_fields_text_map = {
        "publication_id": "SERIAL_NUMBER",
        "description": "PublicationName",
        "supplier_id": "SupplierNum",
        "supplier": "SupplierName",
        "contact": "ContactPersonName",
        "publisher": "PUBLISHER",
        "contact_email": "ContactPersonEmail",
        "claim_date": "ClaimDate",
        "last_update_date": "UpdateDate",
        "reason": "PtorReason",
        "source_currency": "Currency",
        "regulation": "Regulation",
        "volume": "TotalAmount",
        "subjects": "PublicationSUBJECT",
        "start_date": "StartDate",
        "end_date": "EndDate",
        "decision": "Decision",
        "page_title": "PublicationType",
    }
    source_data = {
        k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in input_fields_text_map.items()}
    publication_id = publication_id_from_url(row["url"])
    if str(publication_id) != str(source_data["publication_id"]):
        raise Exception("invalid or blocked response (%s != %s)" % (publication_id, source_data["publication_id"]))
    return {
        "publisher_id": int(row["id"]),
        "publication_id": publication_id,
        "tender_type": "exemptions",
        "page_url": row["url"],
        "description": source_data["description"],
        "supplier_id": source_data["supplier_id"],
        "supplier": source_data["supplier"],
        "contact": source_data["contact"],
        "publisher": source_data["publisher"],
        "contact_email": source_data["contact_email"],
        "claim_date": parse_datetime(source_data["claim_date"]),
        "last_update_date": parse_date(source_data["last_update_date"]),
        "reason": source_data["reason"],
        "source_currency": source_data["source_currency"],
        "regulation": source_data["regulation"],
        "volume": source_data["volume"],
        "subjects": source_data["subjects"],
        "start_date": parse_date(source_data["start_date"]),
        "end_date": parse_date(source_data["end_date"]),
        "decision": source_data["decision"],
        "page_title": source_data["page_title"],
        "tender_id": "none",
        "documents": json.dumps(documents, sort_keys=True, ensure_ascii=False)
    }

def get_office_data(row, page, documents):
    input_fields_text_map = {
        "publication_id": "SERIAL_NUMBER",
        "publishnum": "PublishNum",
        "description": "PublicationName",
        "publisher": "Publisher",
        "claim_date": "ClaimDate",
        "last_update_date": "UpdateDate",
        "subjects": "PublicationSUBJECT",
        "publish_date": "PublishDate",
        "status": "PublicationSTATUS"
    }
    source_data = {
        k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in input_fields_text_map.items()}
    publication_id = publication_id_from_url(row["url"])
    if str(publication_id) != str(source_data["publication_id"]):
        raise Exception("invalid or blocked response")
    return {
        "publisher_id": int(row["id"]),
        "publication_id": publication_id,
        "tender_type": "office",
        "page_url": row["url"],
        "description": source_data["description"],
        "supplier_id": None,
        "supplier": None,
        "contact": None,
        "publisher": source_data["publisher"],
        "contact_email": None,
        "claim_date": parse_datetime(source_data["claim_date"]),
        "last_update_date": parse_date(source_data["last_update_date"]),
        "reason": None,
        "source_currency": None,
        "regulation": None,
        "volume": None,
        "subjects": source_data["subjects"],
        "start_date": parse_date(source_data["publish_date"]),
        "end_date": None,
        "decision": source_data["status"],
        "page_title": None,
        "tender_id": source_data["publishnum"] or 'none',
        "documents": json.dumps(documents, sort_keys=True, ensure_ascii=False),
    }

def get_central_data(row, page, documents):
    # michraz_number = page("#ctl00_PlaceHolderMain_MichraznumberPanel div.value").text().strip()
    documents = []
    for elt in page("#ctl00_PlaceHolderMain_SummaryLinksPanel_SummaryLinkFieldControl1__ControlWrapper_SummaryLinkFieldControl a"):
        documents.append({"description": ' '.join(elt.text.strip().split()),
                            "link": elt.attrib["href"],
                            "update_time": None})
    for elt in page("#ctl00_PlaceHolderMain_SummaryLinks2Panel"):
        documents.append({"description": ' '.join(pq(elt).text().strip().split()),
                            "link": pq(elt).find("a")[0].attrib["href"],
                            "update_time": None})
    publication_id = page("#ctl00_PlaceHolderMain_ManofSerialNumberPanel div.value").text().strip()
    outrow = {
        "publisher_id": None,
        "publication_id": int(publication_id) if publication_id else 0,
        "tender_type": "central",
        "page_url": row["url"],
        "description": page("#ctl00_PlaceHolderMain_GovXContentSectionPanel_Richhtmlfield1__ControlWrapper_RichHtmlField").text().strip(),
        "supplier_id": None,
        "supplier": page("#ctl00_PlaceHolderMain_GovXParagraph1Panel_ctl00__ControlWrapper_RichHtmlField div").text().strip(),
        "contact": page("#ctl00_PlaceHolderMain_WorkerPanel_WorkerPanel1 div.worker").text().strip(),
        "publisher": None,
        "contact_email": None,
        "claim_date": None,
        "last_update_date": None,
        "reason": None,
        "source_currency": None,
        "regulation": page("#ctl00_PlaceHolderMain_MIchrazTypePanel div.value").text().strip(),
        "volume": None,
        "subjects": page("#ctl00_PlaceHolderMain_MMDCategoryPanel div.value").text().strip(),
        "start_date": None,
        "end_date": parse_date(page("#ctl00_PlaceHolderMain_TokefEndDatePanel div.Datevalue").text().strip()),
        "decision": page("#ctl00_PlaceHolderMain_MichrazStatusPanel div.value").text().strip(),
        "page_title": page("h1.MainTitle").text().strip(),
        "tender_id": tender_id_from_url(row["url"]),
        "documents": json.dumps(documents, sort_keys=True, ensure_ascii=False),
    }
    if outrow["description"] == "" and outrow["supplier"] == "" and outrow["subjects"] == "":
        raise Exception("invalid or blocked response")
    return outrow


def _get_url_response_text(url, timeout=180):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) ' +
                        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                        'Chrome/54.0.2840.87 Safari/537.36'
    }
    response = requests.get(url, timeout=timeout, headers=headers)
    response.raise_for_status()
    return response.text


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if not row['__is_stale']:
        return 

    stats.setdefault('handled-urls', 0)
    if stats['handled-urls'] >= 5000:
        return

    try:
        stats['handled-urls'] += 1
        url = row['url']
        if not url.startswith("http"):
            url = '{}{}'.format(url_prefix, url)
        row['url'] = url
        data=_get_url_response_text(url)
    except IOError:
        stats.setdefault('failed-urls', 0)
        stats['failed-urls'] += 1
        logging.exception('Failed to load %s', url)
        return

    try:
        page = pq(data)
        documents = []
        documents_valid = True
        for update_time_elt, link_elt, img_elt in zip(page("#ctl00_PlaceHolderMain_pnl_Files .DLFUpdateDate"),
                                                      page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData a"),
                                                      page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData img")):
            update_time = parse_date(update_time_elt.text.split()[-1])
            if update_time is not None:
                update_time = update_time
            link = get_document_link(BASE_URL, link_elt.attrib.get("href", ""))
            if link is None:
                documents_valid = False
            documents.append({"description": ' '.join(img_elt.attrib.get("alt", "").replace(r'\n', ' ').split()),
                              "link": link,
                              "update_time": update_time})
        if not documents_valid:
            return
        if row["tender_type"] == "exemptions":
            row.update(get_exemptions_data(row, page, documents))
        elif row["tender_type"] == "office":
            row.update(get_office_data(row, page, documents))
        elif row["tender_type"] == "central":
            row.update(get_central_data(row, page, documents))
        else:
            raise Exception("invalid tender_type: {}".format(row["tender_type"]))
        del row['id']
        del row['url']
        return row
    except:
        logging.exception('Failed to parse data from %s', row.get('url'))



def modify_datapackage(dp, *_):
    dp['resources'][0]['name'] = 'tenders'
    dp['resources'][0]['schema']['primaryKey'] = TABLE_SCHEMA['primaryKey']
    fields = dp['resources'][0]['schema']['fields']
    fields = list(filter(lambda x: x['name'].startswith('_'), fields))
    fields.extend(TABLE_SCHEMA['fields'])
    dp['resources'][0]['schema']['fields'] = fields
    return dp

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)