-- The processed files (in the budgetkey-files bucket) each budget item was loaded from, per year.
-- A query of its own, narrow and grouped once: adding this to query.sql overflows datacity's temp_file_limit
-- when the result is streamed through a cursor. The functional-classification rollups get theirs in the fetcher.
SELECT "municipality-code" AS muni_code,
       "card-code" AS code,
       "fiscal-year" AS YEAR,
       array_agg(DISTINCT _source ORDER BY _source) AS source_files
FROM muni_budgets
GROUP BY 1, 2, 3
