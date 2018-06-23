from datapackage_pipelines.wrapper import process

url_prefix="https://www.mr.gov.il"


def process_row(row, *_):
    url = row['url']
    if not url.startswith("http"):
        row['url'] = '{}{}'.format(url_prefix, url)
    return row


if __name__ == '__main__':
    process(process_row=process_row)
