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
    key_fields = ['year', 'leading_item', 'req_code']
    resource = datapackage['resources'][0]
    resource['name'] = 'transactions'
    fields = resource['schema']['fields']
    new_fields = []
    for field in fields:
        name = field['name']
        if name in key_fields:
            new_fields.append(field)
    new_fields.append({
        'name': 'transaction_id',
        'type': 'string',
        'es:keyword': True
    })
    resource['schema']['fields'] = new_fields
    return datapackage


def transfer_code(change):
    try:
        return '%d/%02d-%03d' % (
            change['year'],
            change['leading_item'],
            change['req_code']
        )
    except:
        logging.error('Failed to extract code from change %r', change)
        raise


def get_changes(rows):
    for row in rows:
        row['trcode'] = transfer_code(row)

        if row['date'] is None:
            if row['pending']:
                row['date_kind'] = 'pending'
            else:
                row['date_kind'] = row['trcode']
        else:
            row['date_kind'] = row['date'].isoformat()

        yield row


def get_transactions(changes):
    def get_date(x):
        return x['date_kind']

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
        changes = [
            c for c in changes
            if c['leading_item'] != 47
        ]

        reserve_changes = [
            c for c in changes
            if c['budget_code'].startswith('0047')
            if any(c[field] for field in value_fields)
        ]

        all_trcodes = {c['trcode'] for c in changes}
        other_change_trcodes = all_trcodes - {c['trcode'] for c in reserve_changes}
        logging.debug('Non-Reserve change count: %d', len(other_change_trcodes))
        for trcode in sorted(other_change_trcodes):
            yield [trcode]

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

            for rest in rest_changes:
                yield [rest['trcode']]

    def assign_transactions(groups, changes):
        # This allows to perform search ~10 times faster
        changes_by_trcode = {}
        for x in changes:
            if x['budget_code'].startswith('0047'):
                continue
            trcode = x['trcode']
            changes_by_trcode.setdefault(trcode, []).append(x)

        processed_count = 0
        for trcodes in groups:
            transfer_changes = []
            date_kinds = set()
            trcodes = sorted(list(trcodes))
            for trcode in trcodes:
                changes = changes_by_trcode[trcode]
                transfer_changes.extend(changes_by_trcode[trcode])
                date_kinds.update(change['date_kind'] for change in changes)

            # Sanity check
            assert len(date_kinds) == 1
            date_kind = date_kinds.pop()

            transaction = {
                'id': trcodes[0],
                'changes': set((c['year'], c['leading_item'], c['req_code']) for c in transfer_changes)
            }

            if len(trcodes) > 1:
                for trcode in trcodes:
                    per_transfer_changes = changes_by_trcode[trcode]
                    s = sum(
                        sum(x[f] for f in value_fields)
                        for x in per_transfer_changes
                    )
                    if s > 0:
                        transaction['id'] = trcode
                        logging.debug(
                            '%s: Selected transaction id %s as '
                            'representative for %r' %
                            (date_kind, transaction['id'], trcodes))
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
            ret = {
                'year': change_key[0],
                'leading_item': change_key[1],
                'req_code': change_key[2],
                'transaction_id': transaction['id']
            }
            yield ret


def process_resources(resources):
    for res in resources:
        yield process_resource(res)


def main():
    parameters, datapackage, res_iter = ingest()
    spew(update_datapackage(datapackage), process_resources(res_iter))


main()
