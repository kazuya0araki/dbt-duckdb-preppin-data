-- Output 1: Total Values of Transactions by each bank
SELECT
  bank_code,
  sum(value) AS total_value
FROM
  {{ ref("2023__week_1__pd_2023_wk_1_output_intermediate") }}
GROUP BY
  ALL
