CREATE OR REPLACE VIEW updated_covid_stats AS 
SELECT
  "date"
, "country"
, "confirmed"
, (CASE WHEN ("new_cases" > 0) THEN "new_cases" ELSE 0 END) "new_cases"
, "deaths"
, (CASE WHEN ("new_deaths" > 0) THEN "new_deaths" ELSE 0 END) "new_deaths"
, "recovered"
, (CASE WHEN ("new_recovered" > 0) THEN "new_recovered" ELSE 0 END) "new_recovered"
, "active"
, COALESCE("case_fatality_ratio", 0.0) "case_fatality_ratio"
FROM
  (
   SELECT
     "date"
   , "country"
   , "confirmed"
   , ("confirmed" - "lag"("confirmed", 1, "confirmed") OVER (PARTITION BY "country" ORDER BY "date" ASC)) "new_cases"
   , "deaths"
   , ("deaths" - "lag"("deaths", 1, "deaths") OVER (PARTITION BY "country" ORDER BY "date" ASC)) "new_deaths"
   , "recovered"
   , ("recovered" - "lag"("recovered", 1, "recovered") OVER (PARTITION BY "country" ORDER BY "date" ASC)) "new_recovered"
   , (("confirmed" - "recovered") - "deaths") "active"
   , TRY_CAST(((TRY_CAST("deaths" AS double) / TRY_CAST("confirmed" AS double)) * 100.0) AS decimal(5,2)) "case_fatality_ratio"
   FROM
     (
      SELECT
        "concat"("month", '-', "day") "date"
      , "country"
      , "sum"("confirmed") "confirmed"
      , "sum"("deaths") "deaths"
      , "sum"("recovered") "recovered"
      FROM
        etl1_aggregated
      GROUP BY "country", "month", "day"
   ) 
) 
