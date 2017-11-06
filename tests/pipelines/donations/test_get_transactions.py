from datapackage_pipelines_budgetkey.pipelines.donations.get_transactions import GetTransactions
import json, os


class MockGetTransactions(GetTransactions):

    def requests_post(self, url, data):
        d = data.get("d", '{}')
        d = json.loads(d)
        entity_id = d.get("EntityID")
        from_date = d.get("FromDate")
        to_date = d.get("ToDate")
        if entity_id == "20"  or entity_id == "260":
            filename = "statements_mevaker_donations_entity_{}_from_{}_to_{}".format(entity_id,
                                                                                     from_date.replace("/", "-"),
                                                                                     to_date.replace("/", "-"))
        else:
            raise Exception("unknown url / data : {} / {}".format(url, data))
        filename = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.exists(filename):
            with open(filename, "w") as f:
                json.dump(super(MockGetTransactions, self).requests_post(url, data), f)
        with open(filename) as f:
            return json.load(f)


def test_donations_get_transactions():
    resource = list(MockGetTransactions().get_transactions([{"ID": 20,  # candidate id
                                                             "Party": "מפלגת העבודה הישראלית"},
                                                            {"ID": 260,  # candidate id
                                                             "Party": "הליכוד תנועה לאומית"}]))
    assert len(resource) == 6553
    assert resource[0] == {'CandidateName': 'שי נחמן',
                           'City': 'Booca Raton',
                           'Country': 'ארה"ב-FL',
                           'GD_Date': '/Date(1456783200000)/',
                           'GD_Name': 'Adam   R Mizrachi',
                           'GD_Sum': '10640.00',
                           'GuaranteeOrDonation': 'תרומה',
                           'ID': None,
                           'IsControl': False,
                           'IsUpdate': False,
                           'Party': 'מפלגת העבודה הישראלית',
                           'PublisherTypeID': 2,
                           'State': 0,
                           'SumInCurrency': '2800.00 USD',
                           'URL': None}
    assert resource[500] == {'CandidateName': 'הנגבי צחי',
                             'City': 'נשר',
                             'Country': '',
                             'GD_Date': '/Date(1323208800000)/',
                             'GD_Name': 'יוסי עמר',
                             'GD_Sum': '18000.00',
                             'GuaranteeOrDonation': 'תרומה',
                             'ID': None,
                             'IsControl': False,
                             'IsUpdate': False,
                             'Party': 'הליכוד תנועה לאומית',
                             'PublisherTypeID': 2,
                             'State': 0,
                             'SumInCurrency': '0.00',
                             'URL': None}
