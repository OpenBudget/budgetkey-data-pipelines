import tabulator
import logging

from datapackage_pipelines.wrapper import process

kinds = {
    'central': 'מכרז מרכזי',
    'office': 'מכרז משרדי',
    'exemptions': 'רכש פטור ממכרז',
}

tender_conversion_table = tabulator.Stream('https://docs.google.com/spreadsheets/d/1JkZooBwoxKPlrXWWwUtNHLMdxc-iG0n5tdGZ23I8fYY/edit#gid=1250185114',headers=1).open().iter(keyed=True)
tender_conversion_table = dict(
    ((x['tender_type'].strip(), x['decision'].strip(), x['has_awardees'].strip().lower() == 'true', x['has_active_awardees'].strip().lower() == 'true'),
     dict(
         (k, x[k].strip())
         for k in [
             'simple_decision', 'simple_decision_long', 'extended_status', 'tip1', 'tip1_link', 'tip2', 'tip2_link'
         ]
     ))
    for x in tender_conversion_table 
)

exemption_conversion_table = tabulator.Stream('https://docs.google.com/spreadsheets/d/1lM2Afiw2JMJzHMIvWt3XylWLy2PLmVGbqaR9laBXFNo/edit#gid=1604640775', headers=1).open().iter(keyed=True)
exemption_conversion_table = dict(
    ((x['regulation'].strip(), x['decision'].strip(), x['has_awardees'].strip().lower() == 'true', x['has_active_awardees'].strip().lower() == 'true'),
     dict(
         (k, x[k].strip())
         for k in [
             'simple_decision', 'simple_decision_long', 'extended_status', 'tip1', 'tip1_link', 'tip2', 'tip2_link'
         ]
     ))
    for x in exemption_conversion_table 
)

match_errors = set()

def process_row(row, *_):
    # Fix description, publisher, snippet
    if not row['description'] and row['page_title']:
        row['description'] = row['page_title']
    snippet = ''
    if not row['publisher']:
        row['publisher'] = 'מינהל הרכש הממשלתי'
    snippet += row['publisher'] + ': ' + kinds[row['tender_type']]
    if row['description']:
        snippet += ' - ' + row['description']
    row['snippet'] = snippet

    awardees = row['awardees']
    has_awardees = len(awardees) > 0
    has_active_awardees = any(awardee['active'] for awardee in awardees)
    if row['tender_type'] == 'exemptions':
        key = 'regulation'
        lookup = exemption_conversion_table
    else:
        key = 'tender_type'
        lookup = tender_conversion_table
    key = (
        row[key] if row[key] else '',
        row['decision'] if row['decision'] else '',
        has_awardees,
        has_active_awardees
    )
    try:
        update = lookup[key]
        row['simple_decision'] = update['simple_decision']
        row['simple_decision_long'] = update['simple_decision_long']
        row['extended_status'] = update['extended_status']
        tips = []
        if update['tip1']:
            tips.append((update['tip1'], update['tip1_link']))
        if update['tip2']:
            tips.append((update['tip2'], update['tip2_link']))
        row['actionable_tips'] = tips
    except:
        global match_errors
        match_errors.add(repr(key))
        row['simple_decision'] = None
        row['simple_decision_long'] = None
        row['extended_status'] = None
        row['actionable_tips'] = []

    # # Simplify Decision
    decision = row['decision']
    # simple_decision = decision
    # if decision in ('חדש', 'עודכן', 'פורסם וממתין לתוצאות'):
    #   simple_decision = 'מכרז פתוח'
    # elif decision in ('הסתיים', 'בתוקף'):
    #   simple_decision = 'מכרז שנסגר'
    # elif decision == 'בוטל':
    #   simple_decision = 'מכרז שבוטל'
    # elif decision == 'עתידי':
    #   simple_decision = 'מכרז עתידי'
    # elif decision == 'לא בתוקף':
    #   simple_decision = 'הושלם תהליך הרכש'
    # elif not decision:
    #   simple_decision = None
    # elif decision.startswith('אושר '):
    #   simple_decision = 'הושלם תהליך הרכש'
    # elif decision.startswith('לא אושר '):
    #   simple_decision = 'לא אושר'
    # elif decision.startswith('התקשרות בדיעבד '):
    #   simple_decision = 'הושלם תהליך הרכש'
    # elif row['tender_type'] == 'exemptions':
    #   simple_decision = 'בתהליך'
    # row['simple_decision'] = simple_decision
    # row['simple_decision_long'] = simple_decision

    # # Extended Status
    # extended_status = simple_decision
    # awardees = row['awardees']
    # if decision == 'הסתיים' or decision == 'בתוקף':
    #     if not awardees:
    #         extended_status = 'הושלם תהליך הרכש - לא החלה התקשרות'
    #     elif len(awardees) > 0:
    #         if any(awardee['active'] for awardee in awardees):
    #             extended_status = 'הושלם תהליך הרכש - החלה התקשרות'
    #         else:
    #             extended_status = 'הושלם תהליך הרכש והושלמה ההתקשרות'
    # row['extended_status'] = extended_status

    # Awardees text
    awardees_text = None
    if decision == 'בוטל':
        awardees_text = 'המכרז בוטל'
    else:
        if not awardees:
            awardees_text = row.get('entity_name') or row.get('supplier')
        else:
            if len(awardees) == 1:
                awardees_text = awardees[0]['entity_name']
            else:
                awardees_text = '{} +{}'.format(awardees[0]['entity_name'], len(awardees) - 1)
    row['awardees_text'] = awardees_text

    # Hebrew tender type
    row['tender_type_he'] = {
      'office': 'מכרז משרדי',
      'central': 'מכרז מרכזי',
      'exemptions': 'בקשת פטור ממכרז',
    }.get(row['tender_type'])    

    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([dict(
        name='snippet',
        type='string',
        **{'es:index': False}
    ), dict(
        name='simple_decision',
        type='string'
        **{'es:keyword': True}
    ), dict(
        name='simple_decision_long',
        type='string'
        **{'es:keyword': True}
    ), dict(
        name='extended_status',
        type='string'
        **{'es:keyword': True}
    ), dict(
        name='awardees_text',
        type='string'
        **{'es:index': False}
    ), dict(
        name='tender_type_he',
        type='string'
        **{'es:keyword': True}
    ), {
        'name': 'actionable_tips',
        'type': 'array',
        'es:itemType': 'object',
        'es:index': False
    }])
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
    for msg in sorted(match_errors):
        logging.error(msg)
