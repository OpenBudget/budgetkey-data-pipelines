import os
import json
import tempfile

from datapackage_pipelines.wrapper import process

def modify_datapackage(dp, parameters, _):
    os.makedirs(parameters['out-path'], exist_ok=True)
    if dp:
        filename = os.path.join(parameters['out-path'], 'datapackage.json')
        with open(filename + '.tmp', 'w') as tmp:
            json.dump(dp, tmp)
        os.rename(filename+'.tmp', filename)
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage)