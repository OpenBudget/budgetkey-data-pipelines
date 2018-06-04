import os
from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..')


class GuidestarFixtureTests(ProcessorFixtureTestsBase):

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH, 'datapackage_pipelines_budgetkey', 'pipelines', processor.strip() + '.py')

for filename, _func in GuidestarFixtureTests(os.path.join(os.path.dirname(__file__), 'fixtures')).get_tests():
    globals()['test_guidestar_%s' % filename] = _func

