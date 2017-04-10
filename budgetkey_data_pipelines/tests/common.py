from unittest import TestCase
from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase
import os
import json
import logging


class BudgetkeyProcessorsFixturesTests(ProcessorFixtureTestsBase):

    def __init__(self, fixtures_path, fixtures_options):
        super(BudgetkeyProcessorsFixturesTests, self).__init__(fixtures_path)
        self._fixtures_options = fixtures_options

    def _get_procesor_env(self):
        env = super(BudgetkeyProcessorsFixturesTests, self)._get_procesor_env()
        env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
        return env

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(os.path.dirname(__file__), os.pardir, processor.strip() + '.py')

    def _replace_data_input_tokens(self, data_in):
        """preliminary support for replacing tokens inside the fixture data input"""
        for file in self._fixtures_options.get("file_input_tokens", []):
            if bytes('{{"__LOAD_FROM_FILE__": "{}"}}'.format(file).encode("utf-8")) in data_in:
                with open(os.path.join(os.path.dirname(__file__), file)) as f:
                    data = b'\n'.join([bytes(json.dumps({"data": line}).encode("utf-8")) for line in f.readlines()])
                data_in = data_in.replace(bytes('{{"__LOAD_FROM_FILE__": "{}"}}'.format(file).encode("utf-8")), data)
        return data_in

    def _load_fixture(self, dirpath, filename):
        data_in, data_out, dp_out, params, processor_file = super(BudgetkeyProcessorsFixturesTests, self)._load_fixture(dirpath, filename)
        data_in = self._replace_data_input_tokens(data_in)
        return data_in, data_out, dp_out, params, processor_file



class BasePipelineTestcase(TestCase):
    """
    provide some common services for testing datapackage pipelines
    """

    maxDiff = None

    def _get_fixture_tests(self, fixtures_path, fixtures_options):
        return BudgetkeyProcessorsFixturesTests(fixtures_path, fixtures_options).get_tests()

    def _run_fixture_tests(self, fixtures_path, fixtures_options):
        [test() for filename, test in self._get_fixture_tests(fixtures_path, fixtures_options)]
