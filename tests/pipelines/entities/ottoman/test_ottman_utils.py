from datapackage_pipelines_budgetkey.pipelines.entities.ottoman.ottoman_utils import process_row


def test_process_row_short_id_return_none():
    row = {'id': '21'}
    result = process_row(row)
    assert result is None


def test_process_row_fix_id_strip_name_address():
    row = {'id': '123456789.0', 'name': '  my name  ', 'address': '  my address  '}
    result = process_row(row)
    assert result['id'] == '123456789'
    assert result['name'] == 'my name'
    assert result['address'] == 'my address'
