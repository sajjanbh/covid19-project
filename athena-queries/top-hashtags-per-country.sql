CREATE OR REPLACE VIEW top_hastags_per_country AS 
SELECT
  "country"
, "day" "date"
, "hastag"
FROM
  (covid_twitter_etl2
CROSS JOIN UNNEST("top10hashtagspercountry") t (hastag))
WHERE ("length"("country") = 2)
