import os
from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase
import pytest

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..')


class CompanyScraperFixtureTests(ProcessorFixtureTestsBase):

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH, 'datapackage_pipelines_budgetkey', 'pipelines', processor.strip() + '.py')


for filename, _func in CompanyScraperFixtureTests(os.path.join(os.path.dirname(__file__), 'fixtures')).get_tests():
    globals()['test_company_scraper_%s' % filename] = pytest.mark.skip(_func)

