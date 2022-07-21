WITH src AS
  (SELECT "municipality-name" AS muni_name,
          "municipality-code" AS muni_code,
          "card-code" AS code,
          "card-name" AS title,
          "fiscal-year" AS YEAR,
          CASE phase WHEN 'מקורי' THEN value ELSE NULL END AS allocated,
          CASE phase WHEN 'מאושר' THEN value ELSE NULL END AS revised,
          CASE phase WHEN 'ביצוע' THEN value ELSE NULL
          END AS executed,
          "functional-classification-moin-level1-code" AS func_1_code,
          "functional-classification-moin-level1-name" AS func_1_name,
          "functional-classification-moin-level2-code" AS func_2_code,
          "functional-classification-moin-level2-name" AS func_2_name,
          "functional-classification-moin-level3-code" AS func_3_code,
          "functional-classification-moin-level3-name" AS func_3_name
   FROM muni_budgets), 
   combined AS
  (SELECT muni_code, muni_name, code, title, YEAR,
          func_1_code, func_1_name, func_2_code, func_2_name, func_3_code, func_3_name,
          max(allocated) AS allocated, max(revised) AS revised, max(executed) AS executed
   FROM src
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
SELECT * FROM combined
UNION ALL
SELECT muni_code, muni_name, func_3_code AS code, func_3_name AS title, YEAR,
       func_1_code, func_1_name, func_2_code, func_2_name, NULL AS func_3_name, NULL AS func_3_code,
       sum(allocated) AS allocated, sum(revised) AS revised, sum(executed) AS executed
FROM combined
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
UNION ALL
SELECT muni_code, muni_name, func_2_code AS code, func_2_name AS title, YEAR,
       func_1_code, func_1_name, NULL AS func_2_code, NULL AS func_2_name, NULL AS func_3_name, NULL AS func_3_code,
       sum(allocated) AS allocated, sum(revised) AS revised, sum(executed) AS executed
FROM combined
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
UNION ALL
SELECT muni_code, muni_name, func_1_code AS code, func_1_name AS title, YEAR,
       NULL AS func_1_code, NULL AS func_1_name, NULL AS func_2_code, NULL AS func_2_name, NULL AS func_3_name, NULL AS func_3_code,
       sum(allocated) AS allocated, sum(revised) AS revised, sum(executed) AS executed
FROM combined
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
