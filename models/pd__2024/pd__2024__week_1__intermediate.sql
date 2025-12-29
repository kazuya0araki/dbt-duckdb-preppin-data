-- Create intermediate output table for 2024: Week 1 - Prep Air's Flow Card
WITH
  -- - Input the data
  input_data AS (
    SELECT
      flight_details,
      flow_card,
      bags_checked,
      meal_type
    FROM
      {{ ref("pd__2024__week_1__input") }}
  ),

  -- - Split the Flight Details field to form:
  --     -  Date -> flight_date
  --     -  Flight Number -> flight_number
  --     -  From -> origin_city
  --     -  To -> destination_city
  --     -  Class -> seat_class
  --     -  Price -> price
  split_flight_details AS (
    SELECT
      regexp_extract(flight_details, '^([^//]+)//([^//]+)//([^-]+)-([^-]+)//([^//]+)//([^//]+)$', 1, 'i') AS flight_date,
      regexp_extract(flight_details, '^([^//]+)//([^//]+)//([^-]+)-([^-]+)//([^//]+)//([^//]+)$', 2, 'i') AS flight_number,
      regexp_extract(flight_details, '^([^//]+)//([^//]+)//([^-]+)-([^-]+)//([^//]+)//([^//]+)$', 3, 'i') AS origin_city,
      regexp_extract(flight_details, '^([^//]+)//([^//]+)//([^-]+)-([^-]+)//([^//]+)//([^//]+)$', 4, 'i') AS destination_city,
      regexp_extract(flight_details, '^([^//]+)//([^//]+)//([^-]+)-([^-]+)//([^//]+)//([^//]+)$', 5, 'i') AS seat_class,
      regexp_extract(flight_details, '^([^//]+)//([^//]+)//([^-]+)-([^-]+)//([^//]+)//([^//]+)$', 6, 'i') AS price,
      flow_card,
      bags_checked,
      meal_type
    FROM
      input_data
  ),

  -- - Convert the following data fields to the correct data types:
  --     - Date to a date format
  --     - Price to a decimal value
  -- - Change the Flow Card field to Yes / No values instead of 1 / 0
  final AS (
    SELECT
      try_cast(flight_date AS date) AS flight_date,
      flight_number,
      origin_city,
      destination_city,
      seat_class,
      try_cast(price AS decimal(20, 14)) AS price,
      CASE flow_card
        WHEN 1 THEN 'Yes'
        WHEN 0 THEN 'No'
        ELSE 'Unknown'
      END AS flow_card,
      try_cast(bags_checked AS integer) AS bags_checked,
      meal_type
    FROM
      split_flight_details
  )

SELECT * FROM final
