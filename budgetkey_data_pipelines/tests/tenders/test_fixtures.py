from ..common import BasePipelineTestcase
import os


class TendersFixturesTestCase(BasePipelineTestcase):

    def test_fixtures(self):
        self._run_fixture_tests(os.path.join(os.path.dirname(__file__), "fixtures"),
                                {"file_input_tokens": ["tenders/SearchExemptionMessages.aspx"]})
