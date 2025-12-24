-- Output 3: Total Values by Bank and Customer Code
SELECT
  bank_code,
  customer_code,
  sum(value) AS total_value
FROM
  {{ ref("2023__week_1__pd_2023_wk_1_output_intermediate") }}
GROUP BY ALL
