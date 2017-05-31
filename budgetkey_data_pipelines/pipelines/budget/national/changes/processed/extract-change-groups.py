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
        combinations(s, r)
        for r in range(2, min(len(s) + 1, 6))
    )


def subsets(s):
    return map(set, powerset(s))


def combinations(iterable, r):
    # combinations('ABCD', 2) --> AB AC AD BC BD CD
    # combinations(range(4), 3) --> 012 013 023 123
    def get_next(next_of, i):
        ret = nexts.get(next_of, next_of + 1)
        if ret > i + n - r:
            ret = i + n - r
        return ret

    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    nexts = dict(zip(range(n - 1), range(1, n)))
    indices = list(range(r))
    while True:
        found = (yield tuple(pool[i] for i in indices))
        if found:
            for i in indices:
                target = nexts.get(i)
                sources = (k for k, v in nexts.items() if v == i)
                if target is not None:
                    for src in sources:
                        nexts[src] = target
                else:
                    for src in list(sources):
                        del nexts[src]
            indices[0] = get_next(indices[0], 0)
            for j in range(1, r):
                indices[j] = get_next(indices[j - 1], j)
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] = get_next(indices[i], i)
        for j in range(i + 1, r):
            indices[j] = get_next(indices[j - 1], j)


def get_changes(rows):
    result = []

    for row in rows:
        change = dict(row)

        # if change['year'] is None:
        #     change['year'] = datetime.strptime(
        #         change['date/approval'], '%d/%m/%Y').year

        change['trcode'] = transfer_code(change)
        change['_value'] = sum(change.get(x, 0) for x in value_fields)
        for k, v in change.items():
            if k.startswith('date'):
                if v is None:
                    v = datetime.now().strftime('%d/%m/%Y')
                    kind = 'pending'
                    change['date/pending'] = v
                    change['pending'] = True
                else:
                    kind = k[5:]
                change['date'] = v
                change['pending'] = kind == 'pending'
                change['date_kind'] = kind + '/' + v
                break
        result.append(change)

    return result


def get_groups(changes):
    def get_date(x):
        return x['date_kind']

    def get_keys_for_prefix(ch, prefix):
        return set(
            (x['leading_item'], x['req_code'])
            for x in ch if x['budget_code'].startswith(prefix)
        )

    def initialize_groups(changes):
        result = []
        selected_transfer_codes = set()
        for date_kind, date_changes in itertools.groupby(changes, get_date):
            date_changes = list(date_changes)
            date = date_changes[0]['date']
            date_reserve = [
                c for c in date_changes
                if c['budget_code'].startswith('0047') and c['leading_item'] != 47
                if sum(c[field] * c[field] for field in value_fields) > 0
            ]
            logging.debug('reserve date: kind:%s num:%s' % (
                date_kind, len(date_reserve)
            ))
            num_found = 0
            i = 0
            for comb_size in range(2, min(len(date_reserve) + 1, 7)):
                done = False
                while not done:
                    not_selected = list(
                        x for x in date_reserve
                        if x['trcode'] not in selected_transfer_codes
                    )
                    logging.debug('len(not_selected)=%d' % len(not_selected))
                    date_groups = combinations(not_selected, comb_size)
                    found = None
                    done = True
                    try:
                        while True:
                            i += 1
                            if i % 100000 == 0:
                                logging.debug('%s %s %s %s' % (
                                    date, len(date_reserve), num_found, i
                                ))
                            group = date_groups.send(found)
                            found = False
                            sumvec = sum(
                                sum(c.get(x, 0) for c in group) ** 2
                                for x in value_fields
                            )
                            if sumvec == 0:
                                pending = group[0]['pending']
                                transfer_codes = set(x['trcode'] for x in group)
                                if len(selected_transfer_codes & transfer_codes) > 0:
                                    continue
                                selected_transfer_codes.update(transfer_codes)
                                transfer_codes = list(transfer_codes)
                                transfer_codes.sort()
                                num_found += len(transfer_codes)
                                to_append = {
                                    'transfer_ids': transfer_codes,
                                    'date': date,
                                    'pending': pending,
                                }
                                result.append(to_append)
                                found = True
                    except StopIteration:
                        pass
        for change in changes:
            if not change['trcode'] in selected_transfer_codes:
                selected_transfer_codes.add(change['trcode'])
                result.append({
                    'transfer_ids': [change['trcode']],
                    'date': change['date'],
                    'pending': change['pending']
                })
        return result

    def build_groups(groups, changes):
        for group in groups:
            trcodes = set(group['transfer_ids'])
            years = set(
                int(x.split('/')[0])
                for x in trcodes
            )
            assert(len(years) == 1)

            group['transfer_ids'] = list(set(
                x.split('/')[1]
                for x in trcodes
            ))
            group['group_id'] = group['transfer_ids'][0]

            transfer_changes = list(filter(
                lambda x: x['trcode'] in trcodes,
                changes
            ))
            group['budget_codes'] = list(set(
                x['budget_code']
                for x in transfer_changes
            ))
            group['prefixes'] = list(set(
                itertools.chain.from_iterable(
                    [code[:l] for l in range(2, 10, 2)]
                    for code in group['budget_codes']
                )
            ))
            group['prefixes'].sort(key=lambda x: int('1' + x))
            group['changes'] = [
                {'keys': get_keys_for_prefix(transfer_changes, code)}
                for code in group['prefixes']
            ]

            trcodes = list(trcodes)
            trcodes.sort()
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
                    group['group_id'] = trcode.split('/')[1]
                    logging.debug(
                        'selected group id %(group_id)s as '
                        'representative for %(transfer_ids)r' % group)
                    break

        return groups

    changes.sort(key=get_date)
    result = initialize_groups(changes)
    result = build_groups(result, changes)
    return result


def process_resource(rows):
    groups = get_groups(get_changes(rows))
    for group in groups:
        keys = set()
        for change in group['changes']:
            keys.update(change['keys'])

        for key in keys:
            yield {
                'leading_item': key[0],
                'req_code': key[1],
                'transaction_id': group['group_id']
            }


def process_resources(resources):
    for res in resources:
        yield process_resource(res)


def main():
    parameters, datapackage, res_iter = ingest()
    spew(update_datapackage(datapackage), process_resources(res_iter))


main()
