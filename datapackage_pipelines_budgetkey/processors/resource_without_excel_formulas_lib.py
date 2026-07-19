import os
import logging
import requests
import tempfile
import shutil
import tabulator

def iter_resource_without_excel_formulas(url, parameters={}):
    with tempfile.NamedTemporaryFile(suffix='.csv') as out:
        headers = {
            'User-Agent': 'kz-data-reader'
        }
        download = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.basename(url))
        resp = requests.get(url, stream=True, headers=headers)
        shutil.copyfileobj(resp.raw, download)
        download.close()
        download = download.name
        content = open(download, 'rb').read()
        os.unlink(download)

        if content.startswith(b'\x1f\x8b'):
            import gzip
            content = gzip.decompress(content)

        content = content.replace(b'\n="', b'\n"')
        content = content.replace(b',="', b',"')

        out.write(content)
        out.flush()

        logging.info('downloaded from %s %d bytes: %r', url, len(content), content[:1000])
        assert resp.status_code == 200

        stream = \
            tabulator.Stream('file://'+out.name, force_strings=True, **parameters.get('tabulator', {}))\
            .open()
        
        return stream.headers, stream.iter(keyed=True)
