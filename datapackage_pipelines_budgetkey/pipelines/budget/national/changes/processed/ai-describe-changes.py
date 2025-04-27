from datapackage_pipelines_budgetkey.common.cached_openai import complete

def get_prompt(row, change_list):
    PROMPT = """
You are a data analyst. You have been given details of a budget change request. 
The budget change request contains many changes - your job is to provide a concise explanation of a single change.
Your sources are: The details of the budget change request, the explanatory notes attached to the request and the details of the single change itself.

The details of the budget change request are:
"""
    summary_prompt = []
    summary = row['summary']
    summary_prompt.append(f'Request Title: {summary["title"]}')
    summary_prompt.append(f'Change Kind: {summary["kind"]}')
    if summary.get('from'):
        from_items = summary['from']
        from_items = '\n- '.join('{} ({})'.format(x[2], x[0][2:]) for x in from_items)
        summary_prompt.append(f'From:\n- {from_items}')
    if summary.get('to'):
        to_items = summary['to']
        to_items = '\n- '.join('{} ({})'.format(x[2], x[0][2:]) for x in to_items)
        summary_prompt.append(f'To:\n- {to_items}')
    PROMPT += '\n'.join(summary_prompt)
    if row.get('explanation'):
        PROMPT += f"""

The explanatory notes that were provided with the request are:
{row['explanation']}
"""
    change_list = row['change_list'][0]
    change_list_prompt = []
    for k, v in change_list.items():
        if 'diff' in k and v != 0:
            k = k.replace('_', ' ').capitalize() + '.: '
            change_list_prompt.append(f'{k}{v}')
    change_list_prompt = '\n- '.join(change_list_prompt)
    code, title = change_list['budget_code_title'].split(':', 1)
    nice_code = code[2:4] + '-' + code[4:6] + '-' + code[6:8] 
    PROMPT += f"""
The details of the change itself are:
- CODE: {code[2:]} (might also appear as {nice_code})
- TITLE: {title}
- {change_list_prompt}

Please provide:
1. A concise, one sentence explanation of the above change, in Hebrew.
Don't focus on what the change is, but on why it was made. Be as exact and specific as possible, while still being concise and clear and including all relevant details.
2. A description of the budget item itself, as found verbatim in the explanatory notes (sometimes comes after the words "תיאור התוכנית:" or "התכנית נועדה ל").
Don't paraphrase or summarize it, but provide it as is, and only use the text in the explanatory notes (don't include the words "תיאור התוכנית:" or similar).
If such a description is not available, say "UNAVAILABLE".

Provide your answer in JSON format. The JSON object should look like this:
{{
"explanation": "The explanation of the change",
"description": "The description of the budget item itself"
}}

Do not include any other information, embellishments or explanations, but only the JSON object itself.

"""

    return PROMPT    

def explain():
    def func(row):

        if not row.get('change_list'):
            return row
        change_list = row['change_list']            
        for cli in change_list:
            try:
                prompt = get_prompt(row, cli)
            except Exception as e:
                return        

            _, content = complete(prompt, structured=True)
            print('PROMPT:', prompt)
            cli['ai_change_explanation'] = content['explanation']
            cli['ai_budget_item_description'] = content['description']
            print('EXPLANATION:', content['explanation'])
            if cli['ai_budget_item_description'] == 'UNAVAILABLE':
                cli['ai_budget_item_description'] = ''
            else:
                if content['description'] not in prompt:
                    print('ROUGE DESCRIPTION:', content['description'])
        return row
    return func

def flow(*_):
    return DF.Flow(
        explain(),
    )

if __name__ == '__main__':
    CHANGES_EXPLANATIONS_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/changes/full/datapackage.json'
    import dataflows as DF
    rows = DF.Flow(
        DF.load(CHANGES_EXPLANATIONS_SOURCE_DATAPACKAGE),
        DF.checkpoint('ai-describe-changes'),
        DF.filter_rows(lambda row: row['year'] == 2024),
    ).results()[0][0]
    import pprint
    pprint.pprint(explain()(rows[0]))
    
