import os
from ..common import BudgetkeyProcessorsFixturesTests

fixtures_path = os.path.join(os.path.dirname(__file__), "fixtures")
fixtures_options = {"file_input_tokens": ["tenders/SearchExemptionMessages.aspx"]}

globals().update({"test_tenders_fixtures_{}".format(filename): testfunc
                  for filename, testfunc
                  in BudgetkeyProcessorsFixturesTests(fixtures_path, fixtures_options).get_tests()})
