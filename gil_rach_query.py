codes = """
20620101
20620102
20620103
20620104
20620107
20620201
20620202
60021001
60021002
60021009
60021014
206101
206601
20670205
20670210
20670211
20680101
20680102
20680105
21110101
23103896
60021012
60021061
36410101
36410102
36410103
36410105
36410106
36410107
36410114
36410118
36410119
36410120
36410169
36410170
36410171
36410172
36410180
36410181
36410182
23103901
23103920
23103977
23103840
23103894
23103896
23103897
23103898
23103899
23103810
23103812
23103842
23103843
23103856
23103857
23103859
23103895
23103982
24070931
24160362
24070904
24070905
24200303""".strip().splitlines()

#   year AS "שנת תקציב",
#   substr(code, 3) AS "מספר סעיף תקציבי",
#   title_level2 AS "שם משרד",
#   title_level6 AS "שם תכנית",
#   title_level8 AS "שם תקנה",
#   net_allocated AS "תקציב מקורי",
#   net_revised AS "תקציב על שינוייו",
#   net_executed AS "ביצוע"


HEADER_MAPPING = {
    'year': 'שנת תקציב',
    'substr(code, 3)': 'מספר הסעיף התקציבי',
    'title_level2': 'שם משרד',
    'title_level6': 'שם תכנית',
    'title_level8': 'שם תקנה',
    'net_allocated': 'תקציב מקורי',
    'net_revised': 'תקציב על שינוייו',
    'net_executed': 'ביצוע'
}
SELECT = ',\n\t'.join('%s AS "%s"' % i for i in HEADER_MAPPING.items())
CODES = ',\n\t'.join("'00%s'" % code for code in codes)

QUERY = f"""
SELECT {SELECT}
FROM raw_budget
WHERE 
  year <= {{{{max_year}}}} AND
  year >= {{{{min_year}}}} AND
  code IN ({CODES})
ORDER BY year desc, code asc   
"""

if __name__ == '__main__':
    print(QUERY)