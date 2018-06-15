from datapackage_pipelines.wrapper import process

kinds = {
    'central': 'מכרז מרכזי',
    'office': 'מכרז משרדי',
    'exemptions': 'רכש פטור ממכרז',
}

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

    # Simplify Decision
    decision = row['decision']
    simple_decision = decision
    if decision in ('חדש', 'עודכן', 'פורסם וממתין לתוצאות'):
      simple_decision = 'מכרז פתוח'
    elif decision in ('הסתיים', 'בתוקף'):
      simple_decision = 'מכרז שנסגר'
    elif decision == 'בוטל':
      simple_decision = 'מכרז שבוטל'
    elif decision == 'עתידי':
      simple_decision = 'מכרז עתידי'
    elif decision == 'לא בתוקף':
      simple_decision = 'הושלם תהליך הרכש'
    elif not decision:
      simple_decision = None
    elif decision.startswith('אושר '):
      simple_decision = 'הושלם תהליך הרכש'
    elif decision.startswith('לא אושר '):
      simple_decision = 'לא אושר'
    elif decision.startswith('התקשרות בדיעבד '):
      simple_decision = 'הושלם תהליך הרכש'
    elif row['tender_type'] == 'exemptions':
      simple_decision = 'בתהליך'
    row['simple_decision'] = simple_decision
    row['simple_decision_long'] = simple_decision

    # Extended Status
    extended_status = simple_decision
    awardees = row['awardees']
    if decision == 'הסתיים' or decision == 'בתוקף':
        if not awardees:
            extended_status = 'הושלם תהליך הרכש - לא החלה התקשרות'
        elif len(awardees) > 0:
            if any(awardee['acrive'] for awardee in awardees):
                extended_status = 'הושלם תהליך הרכש - החלה התקשרות'
            else:
                extended_status = 'הושלם תהליך הרכש והושלמה ההתקשרות'
    row['extended_status'] = extended_status

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
        type='string'
    ), dict(
        name='simple_decision',
        type='string'
    ), dict(
        name='simple_decision_long',
        type='string'
    ), dict(
        name='extended_status',
        type='string'
    ), dict(
        name='awardees_text',
        type='string'
    ), dict(
        name='tender_type_he',
        type='string'
    )])
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
