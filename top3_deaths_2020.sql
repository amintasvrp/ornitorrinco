/* 
Top 3 countries with most deaths by COVID-19 in 2020
 */
SELECT Country, Deaths, Date FROM measurements_cleaned
WHERE Country IN (SELECT Country FROM measurements_cleaned 
  ORDER BY Date DESC, Deaths DESC LIMIT 6) AND 
  Date > '2020-03-01T00:00:00.000Z' AND 
  Date <= '2020-12-31T00:00:00.000Z';