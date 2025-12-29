-- Output 1: Total Values of Transactions by each bank
SELECT
  bank_code,
  sum(value) AS total_value
FROM
  {{ ref("pd__2023__week_1__intermediate") }}
GROUP BY
  ALL
