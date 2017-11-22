from datapackage_pipelines_budgetkey.pipelines.supports.criteria.parser import parse_row
from datapackage import DataPackage


def test_support_criteria_parser():
    dp = DataPackage({
        "name": "support-criteria",
        "resources": [{
            "name": "criteria",
            "dialect": {"delimiter": ",", "doubleQuote": True, "lineTerminator": "\r\n", "quoteChar": '"', "skipInitialSpace": False}, "encoding": "utf-8", "format": "csv",
            "path": "tests/support/criteria.csv",
            "schema": {
                "fields": [
                    # the original support-criteria fields
                    {"format": "%Y-%m-%d", "name": "date", "type": "date"},
                    {"name": "title", "type": "string"},
                    {"name": "paper_type", "type": "string"},
                    {"name": "office", "type": "string"},
                    {"format": "uri", "name": "pdf_url", "type": "string"},
                    # the expected data from the parser
                    {"name": "expected_purpose", "type": "string"}
                ]
            }
        }]
    })
    i = 0
    num_parsed = 0
    num_expected_purposes = 0
    for i, row in enumerate(dp.resources[0].iter(keyed=True)):
        parsed_row = parse_row(row)
        if len(parsed_row["purpose"]) > 0 and parsed_row["purpose"] != row["title"]:
            num_parsed += 1
        row_expected_purpose = row["expected_purpose"] if row["expected_purpose"] else ""
        if len(row_expected_purpose) > 0:
            num_expected_purposes += 1
            assert parsed_row["purpose"] == row_expected_purpose, "{}".format({"i":i, "row":row, "parsed_row":parsed_row, "expected_purpose":row_expected_purpose})
    assert i == 358
    assert num_expected_purposes > 20, "not enough purposes were checked, might be something wrong with the criteria.csv file"
    assert num_parsed > 190, "not enough parsed rows"
