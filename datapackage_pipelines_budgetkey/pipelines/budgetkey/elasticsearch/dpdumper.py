import os
import json
import tempfile

from datapackage_pipelines.wrapper import process

def modify_datapackage(dp, parameters, _):
    os.makedirs(parameters['out-path'], exist_ok=True)
    if dp:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmp:
            json.dump(dp, tmp)
            tmp.close()
            os.rename(tmp.name, os.path.join(parameters['out-path'], 'datapackage.json'))
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage)