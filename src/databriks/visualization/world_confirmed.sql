/* 
Confirmed Cases by COVID-19 in every country until 09/06/2021
 */
SELECT Country, Confirmed, alpha3_code AS Code 
FROM measurements_cleaned AS m
LEFT JOIN alpha_2_to_3 AS a
WHERE m.CountryCode = a.alpha2_code AND 
m.Date = '2021-06-09T00:00:00.000Z';