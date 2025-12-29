-- Output 3: Total Values by Bank and Customer Code
SELECT
  bank_code,
  customer_code,
  sum(value) AS total_value
FROM
  {{ ref("pd__2023__week_1__intermediate") }}
GROUP BY ALL
