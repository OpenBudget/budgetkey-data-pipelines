import os
import json

from datapackage_pipelines.wrapper import process

def modify_datapackage(dp, parameters, _):
    os.makedirs(parameters['out-path'], exist_ok=True)
    json.dump(dp, open(os.path.join(parameters['out-path'], 'datapackage.json'), 'w'))
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage)