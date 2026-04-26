CREATE OR REPLACE VIEW transportation.gold.fact_trips
AS (

SELECT 
t.id, t.business_date,t.city_id,c.city_name, t.passenger_catergory,
t.distance_kms, t.sales_amt, t.passenger_rating, t.driver_rating,
ca.month, ca.day_of_month, ca.month_name, ca.month_year, ca.quarter, ca.quarter_year, ca.week_of_year, ca.is_weekend, ca.is_weekday, ca.is_holiday as national_holiday
FROM transportation.silver.trips t
INNER JOIN transportation.silver.city c ON c.city_id = t.city_id
INNER JOIN transportation.silver.calendar ca ON ca.date = t.business_date
)