CREATE OR REPLACE VIEW covid_stats_by_coordinates AS 
SELECT
  "country"
, "concat"("month", '-', "day") "date"
, "latitude"
, "longitude"
, "sum"("confirmed") "confirmed"
, "sum"("deaths") "deaths"
, "sum"("recovered") "recoverd"
, (("sum"("confirmed") - "sum"("deaths")) - "sum"("recovered")) "active"
FROM
  etl1_processed
WHERE ((("latitude" IS NOT NULL) AND ("longitude" IS NOT NULL)) AND (CAST("concat"("month", '-', "day") AS date) = (current_date - INTERVAL  '1' DAY)))
GROUP BY "country", "month", "day", "latitude", "longitude"
