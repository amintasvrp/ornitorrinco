/* 
Top 3 countries with most confirmed cases by COVID-19 in 2021
 */
SELECT Country, Confirmed, Date FROM measurements_cleaned
WHERE Country IN (SELECT Country FROM measurements_cleaned 
  ORDER BY Date DESC, Confirmed DESC LIMIT 6) AND 
  Date >= '2021-01-01T00:00:00.000Z' AND 
  Date <= '2021-06-06T00:00:00.000Z';