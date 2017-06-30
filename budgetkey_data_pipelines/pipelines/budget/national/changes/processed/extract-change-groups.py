from datapackage_pipelines.wrapper import spew, ingest
import itertools
import logging


value_fields = [
    'net_expense_diff',
    'gross_expense_diff',
    'allocated_income_diff',
    'commitment_limit_diff',
    'personnel_max_diff'
]


def update_datapackage(datapackage):
    key_fields = ['leading_item', 'req_code']
    resource = datapackage['resources'][0]
    fields = resource['schema']['fields']
    new_fields = []
    for field in fields:
        name = field['name']
        if name in key_fields:
            new_fields.append(field)
    new_fields.append({
        'name': 'transaction_id',
        'type': 'string'
    })
    resource['schema']['fields'] = new_fields
    return datapackage


def transfer_code(change):
    return '%d/%02d-%03d' % (
        change['year'],
        change['leading_item'],
        change['req_code']
    )


def get_changes(rows):
    # now = datetime.now().strftime('%d/%m/%Y')
    for row in rows:
        row['trcode'] = transfer_code(row)

        if row['date/approval'] is None:
            row['date_kind'] = 'pending/%s' % row['trcode']
        else:
            row['date_kind'] = 'approved/' + (
                row['date/approval'].strftime('%d/%m/%Y')
            )

        yield row


def get_transactions(changes):
    def get_date(x):
        return x['date_kind']

    def get_keys_for_prefixes(ch, prefixes):
        result = set()

        for c in ch:
            key = (c['leading_item'], c['req_code'])
            if key not in result:
                for prefix in prefixes:
                    if c['budget_code'].startswith(prefix):
                        result.add(key)
                        break

        return result

    def try_complete_transaction(prefix, skip_leading_items, skip_indexes,
                                 items, start_index, count_needed):
        if count_needed > 1:
            # try combinations
            for i in range(start_index, len(items) - count_needed + 1):
                if i in skip_indexes:
                    continue
                item = items[i]
                if item['leading_item'] in skip_leading_items:
                    continue
                transaction, new_skip_indexes = try_complete_transaction(
                    prefix + [item],
                    skip_leading_items.union({item['leading_item']}),
                    skip_indexes.union({i}),
                    items,
                    i + 1,
                    count_needed - 1
                )
                if transaction is not None:
                    return transaction, new_skip_indexes
        else:
            # we need to find last item; it should turn sum by each
            # field into zero
            checksum = {
                field: sum(c.get(field, 0) for c in prefix)
                for field in value_fields
            }
            for i in range(start_index, len(items)):
                if i in skip_indexes:
                    continue
                item = items[i]
                if item['leading_item'] in skip_leading_items:
                    continue
                matches = all([
                    item[field] + checksum[field] == 0
                    for field in value_fields
                ])
                if matches:
                    return prefix + [item], skip_indexes.union({i})
        return None, skip_indexes

    def find_all_transactions(items, count_needed):
        transactions = []
        used_indexes = set()
        for i in range(0, len(items) - count_needed + 1):
            if i not in used_indexes:
                # if count_needed >= 5:
                #     logging.debug('%s / %s' % (i, len(items)))
                item = items[i]
                transaction, new_used_indexes = try_complete_transaction(
                    [item],
                    {item['leading_item']},
                    used_indexes.union({i}),
                    items,
                    i + 1,
                    count_needed - 1
                )
                if transaction is not None:
                    used_indexes = new_used_indexes
                    transactions.append(transaction)

        unmatched = []
        for i in range(0, len(items)):
            if i not in used_indexes:
                unmatched.append(items[i])

        return transactions, unmatched

    def find_groups(changes):
        reserve_changes = [
            c for c in changes
            if c['budget_code'].startswith('0047') and c['leading_item'] != 47
            if any(c[field] for field in value_fields)
        ]
        groups = itertools.groupby(reserve_changes, get_date)

        for date_kind, date_reserve in groups:
            date_reserve = list(date_reserve)
            logging.debug('Reserve date: kind: %s, count: %s' % (
                date_kind, len(date_reserve)
            ))
            rest_changes = date_reserve
            for comb_size in range(2, min(len(date_reserve) + 1, 7)):
                if len(rest_changes) < comb_size:
                    break

                transactions, rest_changes = find_all_transactions(
                    rest_changes, comb_size)

                logging.debug(
                    'Combination size: %s, transactions: %s, '
                    'changes left: %s' %
                    (comb_size, len(transactions), len(rest_changes)
                     ))

                for transaction in transactions:
                    yield set(x['trcode'] for x in transaction)

    def assign_transactions(groups, changes):
        # This allows to perform search ~10 times faster
        changes_by_year = {}
        for x in changes:
            year = x['year']
            changes_by_year.setdefault(year, []).append(x)

        processed_count = 0
        for trcodes in groups:
            # Just a check
            years = set(
                int(x.split('/')[0])
                for x in trcodes
            )
            assert(len(years) == 1)
            year = years.pop()

            transfer_changes = list(filter(
                lambda x: x['trcode'] in trcodes,
                changes_by_year[year]
            ))

            budget_codes = set(
                x['budget_code']
                for x in transfer_changes
            )
            prefixes = sorted(set(
                itertools.chain.from_iterable(
                    [code[:l] for l in range(2, 10, 2)]
                    for code in budget_codes
                )
            ), key=lambda x: int('1' + x))

            transfer_ids = list(set(
                x.split('/')[1]
                for x in trcodes
            ))

            transaction = {
                'id': transfer_ids[0],
                'changes': get_keys_for_prefixes(transfer_changes, prefixes),
            }

            if len(trcodes) > 1:
                trcodes = sorted(trcodes)
                for trcode in trcodes:
                    per_transfer_changes = list(filter(
                        lambda x:
                            x['trcode'] == trcode and
                            not x['budget_code'].startswith('0047'),
                        transfer_changes
                    ))
                    s = sum(
                        sum(x[f] for f in value_fields)
                        for x in per_transfer_changes
                    )
                    if s > 0:
                        transaction['id'] = trcode.split('/')[1]
                        logging.debug(
                            'Selected transaction id %s as '
                            'representative for %r' %
                            (transaction['id'], transfer_ids))
                        break

            processed_count += 1
            if processed_count % 1000 == 0:
                logging.debug('Processed: %d' % processed_count)

            yield transaction

    changes = sorted(changes, key=get_date)
    return assign_transactions(find_groups(changes), changes)


def process_resource(rows):
    for transaction in get_transactions(get_changes(rows)):
        for change_key in transaction['changes']:
            yield {
                'leading_item': change_key[0],
                'req_code': change_key[1],
                'transaction_id': transaction['id']
            }


def process_resources(resources):
    for res in resources:
        yield process_resource(res)


def main():
    parameters, datapackage, res_iter = ingest()
    spew(update_datapackage(datapackage), process_resources(res_iter))


main()
