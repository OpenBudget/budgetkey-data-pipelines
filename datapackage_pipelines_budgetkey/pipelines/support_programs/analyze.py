import dataflows as DF

def group_analyze():

    def analyze_group(program_key, rows):
        """Analyze a group of rows with the same key."""
        if not rows:
            return None

        ## Analyze titles
        all_titles = dict()
        for row in rows:
            all_titles.setdefault(row.get('support_title'), set()).add(row.get('year_requested'))
        all_titles = [
            (min(years), max(years), title) for title, years in all_titles.items()
        ]
        all_titles.sort(key=lambda x: x[1])
        title = all_titles[-1][2] if all_titles else None

        ## Budget Codes
        all_budget_codes = set()
        for row in rows:
            budget_codes = row.get('resolved_budget_codes', [])
            for bc in budget_codes:
                all_budget_codes.add((bc.get('year'), bc.get('code'), bc.get('title'), bc.get('doc_id')))
        all_budget_codes = sorted(all_budget_codes, key=lambda x: x[0])

        ## Supporting Ministries
        all_supporting_ministries = dict()
        for row in rows:
            try:
                sm = row['supporting_ministry']
                if sm:
                    all_supporting_ministries.setdefault(sm, set()).add(row.get('year_requested'))
                else:
                    print(f'No supporting ministry for row: {row}')
            except KeyError as e:
                print(f'Missing key {e} in row: {row}')
        all_supporting_ministries = [
            [min(years), max(years), sm] for sm, years in all_supporting_ministries.items()
        ]
        all_supporting_ministries.sort(key=lambda x: x[1])
        supporting_ministry = all_supporting_ministries[-1][2] if all_supporting_ministries else None

        ## Request Types
        all_request_types = set()
        for row in rows:
            all_request_types.add((row.get('year_requested'), row.get('request_type')))
        all_request_types = sorted(all_request_types, key=lambda x: x[0])
        request_type = all_request_types[-1][1] if all_request_types else None

        ## Analyze recipients
        all_recipients = dict()
        for row in rows:
            row['amount_approved'] = float(row.get('amount_approved') or 0)
            row['amount_paid'] = float(row.get('amount_paid') or 0)
            entity_id = row.get('entity_id')
            if entity_id:
                e = all_recipients.setdefault(entity_id, dict(
                    id=entity_id,
                    name=row.get('entity_name'),
                    kind=row.get('entity_kind') or 'other',
                    total_approved=0,
                    total_paid=0,
                    per_year=dict(),
                ))
                e['total_approved'] += row.get('amount_approved')
                e['total_paid'] += row.get('amount_paid')
                year = row.get('year_requested')
                if year:
                    py = e['per_year'].setdefault(year, dict(
                        approved=0,
                        paid=0,
                    ))
                    py['approved'] += row.get('amount_approved')
                    py['paid'] += row.get('amount_paid')
        all_recipients = sorted(all_recipients.values(), key=lambda x: x['total_paid'] or -1, reverse=True)
        for entity in all_recipients:
            for py in entity['per_year'].values():
                py['utilization'] = py['paid'] / py['approved'] if py['approved'] else None
            utilizations = [
                py['utilization'] for py in entity['per_year'].values() if py['utilization'] is not None
            ]
            entity['average_utilization'] = sum(utilizations) / len(utilizations) if utilizations else None
        entity_avg_utilizations = [
            entity['average_utilization'] for entity in all_recipients if entity['average_utilization'] is not None
        ]
        average_utilization = sum(entity_avg_utilizations) / len(entity_avg_utilizations) if entity_avg_utilizations else None

        ## Analyze years
        per_year = dict()
        for row in rows:
            year = row.get('year_requested')
            if year:
                py = per_year.setdefault(year, dict(
                    approved=0,
                    paid=0,
                ))
                py['approved'] += row.get('amount_approved', 0)
                py['paid'] += row.get('amount_paid', 0)
        for year, py in per_year.items():
            entity_utilization_year = [
                e['per_year'].get(year, {}).get('utilization')
                for e in all_recipients
            ]
            entity_utilization_year = [
                u for u in entity_utilization_year if u is not None
            ]
            py['average_utilization'] = sum(entity_utilization_year) / len(entity_utilization_year) if entity_utilization_year else None

        ## Analyze entity kinds
        all_entity_kinds = dict()
        for row in rows:
            entity_kind = row.get('entity_kind') or 'other'
            ek = all_entity_kinds.setdefault(entity_kind, dict(
                kind=entity_kind,
                count=0,
                total_approved=0,
                total_paid=0,
                per_year=dict(),
            ))
            ek['count'] += 1
            ek['total_approved'] += row.get('amount_approved')
            ek['total_paid'] += row.get('amount_paid')
            year = row.get('year_requested')
            if year:
                py = ek['per_year'].setdefault(year, dict(
                    approved=0,
                    paid=0,
                ))
                py['approved'] += row.get('amount_approved')
                py['paid'] += row.get('amount_paid')

        total_approved = sum(ek['total_approved'] for ek in all_entity_kinds.values())
        total_paid = sum(ek['total_paid'] for ek in all_entity_kinds.values())
        for ek in all_entity_kinds.values():
            for py in ek['per_year'].values():
                py['utilization'] = py['paid'] / py['approved'] if py['approved'] else None
            utilizations = [
                py['utilization'] for py in ek['per_year'].values() if py['utilization'] is not None
            ]
            ek['average_utilization'] = sum(utilizations) / len(utilizations) if utilizations else None

            ek['pct'] = ek['count'] / sum(x['count'] for x in all_entity_kinds.values()) if all_entity_kinds else 0
            ek['pct_approved'] = ek['total_approved'] / total_approved if total_approved else 0
            ek['pct_paid'] = ek['total_paid'] / total_paid if total_paid else 0

        max_year = max(row.get('year_requested', 0) for row in rows)
        min_year = min(row.get('year_requested', 0) for row in rows)
        return {
            'program_key': program_key,
            'title': title,
            'all_titles': all_titles,
            'budget_codes': all_budget_codes,
            'supporting_ministry': supporting_ministry,
            'request_type': request_type,
            'all_supporting_ministries': all_supporting_ministries,
            'total_entities': len(all_recipients),
            'total_entity_kinds': len(all_entity_kinds),
            'entity_kinds': list(all_entity_kinds.values()),
            'total_approved': total_approved,
            'total_paid': total_paid,
            'average_utilization': average_utilization,
            'per_year': per_year,
            'min_year': min_year,
            'max_year': max_year,
            'year_range': f'{min_year}-{max_year}' if min_year < max_year else str(min_year),
            'year_span': max_year - min_year + 1,
            'recipients': all_recipients,
        }

    def func(rows):
        current_key = None
        collated = []
        for row in rows:
            key = row.get('program_key')
            if key != current_key:
                if collated:
                    ret = analyze_group(current_key, collated)
                    if ret:
                        yield ret
                current_key = key
                collated = []
            collated.append(row)
        if collated:
            ret = analyze_group(current_key, collated)
            if ret:
                yield ret
    return DF.Flow(
        DF.add_field('title', 'string', **{'es:title': True}),
        DF.add_field('total_entities', 'integer'),
        DF.add_field('total_entity_kinds', 'integer'),
        DF.add_field('total_approved', 'number'),
        DF.add_field('total_paid', 'number'),
        DF.add_field('average_utilization', 'number'),
        DF.add_field('per_year', 'object', **{'es:index': False}),
        DF.add_field('min_year', 'integer'),
        DF.add_field('max_year', 'integer'),
        DF.add_field('year_range', 'string'),
        DF.add_field('year_span', 'integer'),
        DF.add_field('all_titles', 'array', **{'es:itemType': 'string'}),
        DF.add_field('all_supporting_ministries', 'array', **{'es:itemType': 'string'}),
        DF.add_field('budget_codes', 'array', **{'es:index': False, 'es:itemType': 'object'}),
        DF.add_field('entity_kinds', 'array', **{'es:index': False, 'es:itemType': 'object'}),
        DF.add_field('recipients', 'array', **{'es:index': False, 'es:itemType': 'object'}),
        func,
        DF.select_fields([
            'program_key',
            'title',
            'supporting_ministry',
            'request_type',
            'total_entities',
            'total_entity_kinds',
            'total_approved',
            'total_paid',
            'average_utilization',
            'per_year',
            'min_year',
            'max_year',
            'year_range',
            'year_span',
            'all_titles',
            'all_supporting_ministries',
            'budget_codes',
            'entity_kinds',
            'recipients',
        ]),
        DF.set_type('supporting_ministry', **{'es:keyword': True}),
        DF.set_type('request_type', **{'es:keyword': True}),
    )

def score():

    def func(row):
        row['score'] = 1 + row.get('total_approved', 0) / 1000 / (row.get('year_span', 1) ** 0.5)
        return row

    return DF.Flow(
        DF.add_field('score', 'number', default=0),
        func,
    )

def analyze(debug=False):
    return DF.Flow(
        DF.filter_rows(lambda row: row.get('program_key') is not None),
        DF.sort_rows('{program_key}'),
        group_analyze(),
        score(),
        DF.update_resource(-1, name='support_programs', **{'dpp:streaming': True}),
    )

def flow(*_):
    return analyze()

if __name__ == '__main__':
    DF.Flow(
        analyze(debug=True),
        DF.printer(),
        DF.dump_to_path('tmp_support_programs'),
    ).process()

