import requests
import tempfile

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

url = parameters.get('url')
resource = parameters.get('resource')

content = requests.get(url).content
content = content.replace(b'\n="', b'\n"')
content = content.replace(b',="', b',"')

out = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
out.write(content)

resource['url'] = 'file://'+out.name
datapackage['resources'].append(resource)

spew(datapackage, res_iter)
