-- Output 2: Total Values by Bank, Day of the Week and Type of Transaction
SELECT
  bank_code,
  online_or_in_person,
  strftime(transaction_date, '%A') AS transaction_day_of_the_week,
  sum(value) AS total_value
FROM
  {{ ref("2023__week_1__pd_2023_wk_1_output_intermediate") }}
GROUP BY ALL
