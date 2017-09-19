from datapackage_pipelines.generators import GeneratorBase
import os


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object"
        }

    @classmethod
    def generate_pipeline(cls, source):
        stop_after_rows = os.environ.get("ES_LOAD_DATA_ROWS")
        stop_after_rows = int(stop_after_rows) if stop_after_rows else 0
        if os.environ.get("ES_LOAD_DATA"):
            load_data_enabled = True
        else:
            load_data_enabled = stop_after_rows > 0
        for pipeline_id, pipeline_details in source.items():
            if load_data_enabled and pipeline_id.startswith("index_"):
                load_resource_step = pipeline_details["pipeline"][1]
                assert load_resource_step["run"] == "load_resource"
                print(" -- load_data enabled for: {} -- ".format(pipeline_id))
                # remove dependencies and set url to load from http
                url = pipeline_details["pipeline"][1]["parameters"]["url"]
                url = url.replace("/var/datapackages", "http://next.obudget.org/datapackages")
                pipeline_details["pipeline"][1]["parameters"]["url"] = url
                pipeline_details["dependencies"] = []
                if stop_after_rows > 0:
                    pipeline_details["pipeline"] = pipeline_details["pipeline"][:2] + [{
                        "run": "limit_rows",
                        "parameters": {
                            "stop-after-rows": stop_after_rows
                        }
                    }] + pipeline_details["pipeline"][2:]
            yield pipeline_id, pipeline_details
