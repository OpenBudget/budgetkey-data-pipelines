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
        title = all_titles[0][2] if all_titles else None

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
            sm = row.get('supporting_ministry')
            if sm:                
                all_supporting_ministries.setdefault(sm, set()).add(row.get('year_requested'))
        all_supporting_ministries = [
            (min(years), max(years), sm) for sm, years in all_supporting_ministries.items()
        ]
        all_supporting_ministries.sort(key=lambda x: x[1])
        supporting_ministry = all_supporting_ministries[0][2] if all_supporting_ministries else None

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
        for entity in all_recipients.values():
            for py in entity['per_year'].values():
                py['utilization'] = py['paid'] / py['approved'] if py['approved'] else 0
            entity['average_utilization'] = sum(
                py['utilization'] for py in entity['per_year'].values()
            ) / len(entity['per_year']) if entity['per_year'] else 0
        average_utilization = sum(
            entity['average_utilization'] for entity in all_recipients.values()
        ) / len(all_recipients) if all_recipients else 0

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
                py['utilization'] = py['paid'] / py['approved'] if py['approved'] else 0
            ek['average_utilization'] = sum(
                py['utilization'] for py in ek['per_year'].values()
            ) / len(ek['per_year']) if ek['per_year'] else 0

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
            'entities': list(all_entity_kinds.values()),
            'entity_kinds': list(all_entity_kinds.values()),
            'total_approved': total_approved,
            'total_paid': total_paid,
            'average_utilization': average_utilization,
            'min_year': min_year,
            'max_year': max_year,
            'year_range': f'{min_year}-{max_year}' if min_year < max_year else str(min_year),
            'year_span': max_year - min_year + 1,
            'recipients': list(all_recipients.values()),
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
        DF.add_field('title', 'string'),
        DF.add_field('supporting_ministry', 'string'),
        DF.add_field('request_type', 'string'),
        DF.add_field('total_entities', 'integer'),
        DF.add_field('total_entity_kinds', 'integer'),
        DF.add_field('total_approved', 'number'),
        DF.add_field('total_paid', 'number'),
        DF.add_field('average_utilization', 'number'),
        DF.add_field('min_year', 'integer'),
        DF.add_field('max_year', 'integer'),
        DF.add_field('year_range', 'string'),
        DF.add_field('year_span', 'integer'),
        DF.add_field('all_titles', 'array',),
        DF.add_field('all_supporting_ministries', 'array',),
        DF.add_field('budget_codes', 'array',),
        DF.add_field('entities', 'array',),
        DF.add_field('entity_kinds', 'array',),
        DF.add_field('recipients', 'array',),
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
            'min_year',
            'max_year',
            'year_range',
            'year_span',
            'all_titles',
            'all_supporting_ministries',
            'budget_codes',
            'entities',
            'entity_kinds',
            'recipients',
        ])
    )

def analyze(debug=False):
    return DF.Flow(
        DF.filter_rows(lambda row: row.get('program_key') is not None),
        DF.sort_rows('{program_key}'),
        group_analyze(),
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

