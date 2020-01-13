import os
import json

from decimal import Decimal
from dataflows import Flow, DataStreamProcessor
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines.utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY
import datetime

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIME_FORMAT = '%H:%M:%S'
class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime.time):
            return obj.strftime(TIME_FORMAT)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime(DATETIME_FORMAT)
        elif isinstance(obj, datetime.date):
            return obj.strftime(DATE_FORMAT)
        elif isinstance(obj, set):
            return list(obj)
        return super().default(obj)

class DumpToJson(DataStreamProcessor):
    def __init__(self,out_path):
        super(DataStreamProcessor, self).__init__()
        self._out_path = out_path
        self.stats = {}

    def process_resource(self, resource):
        is_first = True
        resource_path = resource.res.infer().get('path', '.')

        out_file = os.path.join(self._out_path, resource_path)
        out_file, _ = os.path.splitext(out_file)

        with open(out_file + '.json', mode='w', encoding="utf-8") as f:
            f.write("[")
            for row in resource:
                if not is_first:
                    f.write(",")
                f.write(json.dumps(row, cls=ExtendedJSONEncoder))
                is_first = False
                yield row
            f.write("]")



def flow(parameters: dict, stats: dict):
    out_path = parameters.pop('out-path', '.')
    stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = os.path.join(out_path, 'datapackage.json')
    return Flow(
        DumpToJson(out_path)
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.stats), ctx)
