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

class RollingJSONFile:
    def __init__(self, file_name, max_rows=0):
        self.num_rows = 0
        self.file_num = 0
        self.base_file_name = file_name
        self.max_rows = max_rows
        self.file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write(self,record):
        if not self.file:
            self.open()

        if self.num_rows >0:
            self.file.write(",")

        self.file.write(json.dumps(record, cls=ExtendedJSONEncoder))
        self.num_rows += 1

        if self.max_rows>0 and self.num_rows >= self.max_rows:
            self.close()
            self.file_num += 1


    def close(self):
        if self.file:
            self.file.write("]")
            self.file.close()
        self.file = None


    def open(self):
        next_file_name = RollingJSONFile.__get_file_name(self.base_file_name, self.file_num)
        self.file = open(next_file_name, mode='w', encoding="utf-8")
        self.num_rows = 0

    @staticmethod
    def __get_file_name(base_file_name, file_num):
        if file_num ==0:
            return base_file_name
        root, ext = os.path.splitext(base_file_name)
        return "{0}{1:02d}{2}".format(root,file_num,ext)



class DumpToJson(DataStreamProcessor):
    def __init__(self,out_path, max_rows):
        super(DataStreamProcessor, self).__init__()
        self.out_path = out_path
        self.max_rows = max_rows
        self.stats = {}

    def process_resource(self, resource):
        resource_path = resource.res.infer().get('path', '.')

        out_file = os.path.join(self.out_path, resource_path)
        out_file, _ = os.path.splitext(out_file)
        if not os.path.exists(os.path.dirname(out_file)):
            try:
                os.makedirs(os.path.dirname(out_file))
            except OSError:
                pass

        with RollingJSONFile(out_file + '.json', self.max_rows) as f:
            for row in resource:
                f.write(row)
                yield row




def flow(parameters: dict, stats: dict):
    out_path = parameters.pop('out-path', '.')
    max_rows = parameters.get('max-rows', 0)
    stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = os.path.join(out_path, 'datapackage.json')
    return Flow(
        DumpToJson(out_path,max_rows)
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.stats), ctx)
