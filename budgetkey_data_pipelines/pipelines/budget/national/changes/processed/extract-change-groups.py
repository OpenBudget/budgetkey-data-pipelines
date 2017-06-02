from datapackage_pipelines.wrapper import spew, ingest
from datetime import datetime
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


def powerset(iterable):
    s = list(iterable)
    return itertools.chain.from_iterable(
        itertools.combinations(s, r)
        for r in range(2, min(len(s) + 1, 6))
    )


def subsets(s):
    return map(set, powerset(s))


def get_changes(rows):
    now = datetime.now().strftime('%d/%m/%Y')
    for row in rows:
        row['trcode'] = transfer_code(row)

        if row['date/approval'] is None:
            row['date_kind'] = 'pending/' + now
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

    def find_groups(changes):
        selected_transfer_codes = set()
        for date_kind, date_changes in itertools.groupby(changes, get_date):
            date_reserve = [
                c for c in date_changes
                if c['budget_code'].startswith('0047') and c['leading_item'] != 47
                if any(c[field] for field in value_fields)
            ]
            logging.debug('Reserve date: kind:%s num:%s' % (
                date_kind, len(date_reserve)
            ))
            num_found = 0  # these two - only to show progress
            i = 0          #
            for comb_size in range(2, min(len(date_reserve) + 1, 7)):
                done = False
                while not done:
                    not_selected = list(
                        x for x in date_reserve
                        if x['trcode'] not in selected_transfer_codes
                    )
                    logging.debug('len(not_selected)=%d' % len(not_selected))
                    date_transactions = itertools.combinations(
                        not_selected, comb_size)
                    found = None
                    done = True
                    try:
                        while True:
                            i += 1
                            if i % 100000 == 0:
                                logging.debug('%s %s %s' % (
                                    len(date_reserve), num_found, i
                                ))
                            transaction = date_transactions.send(found)
                            found = False
                            sumvec = sum(
                                sum(c.get(x, 0) for c in transaction) ** 2
                                for x in value_fields
                            )
                            if sumvec == 0:
                                transfer_codes = set(
                                    x['trcode']
                                    for x in transaction
                                )
                                if len(selected_transfer_codes & transfer_codes) > 0:
                                    continue
                                selected_transfer_codes.update(transfer_codes)
                                transfer_codes = sorted(transfer_codes)
                                num_found += len(transfer_codes)
                                yield set(transfer_codes),
                                found = True
                    except StopIteration:
                        pass
        for change in changes:
            if not change['trcode'] in selected_transfer_codes:
                selected_transfer_codes.add(change['trcode'])
                yield {change['trcode']}

    def assign_transactions(groups, changes):

        # This allows to perform search ~10 times faster
        changes_by_year = {}
        for x in changes:
            year = x['year']
            changes_by_year[year] = changes_by_year.get(year, [])
            changes_by_year[year].append(x)

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
                            'selected transaction id %s as '
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
