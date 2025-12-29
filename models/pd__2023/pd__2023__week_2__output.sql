SELECT
  transaction_id,
  CONCAT(
    country_code,
    check_digits,
    bank_code,
    sort_code,
    account_number
  ) AS iban,
FROM
  {{ ref("pd__2023__week_2__intermediate") }}
