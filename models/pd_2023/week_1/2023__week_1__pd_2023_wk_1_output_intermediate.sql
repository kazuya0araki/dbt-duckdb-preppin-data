WITH
  -- - Input the data
  input_data AS (
    SELECT
      "Transaction Code" AS transaction_code,
      value,
      "Customer Code" AS customer_code,
      "Online or In-Person" AS online_or_in_person,
      "Transaction Date" AS transaction_date
    FROM
      {{ ref("2023__week_1__pd_2023_wk_1_input") }}
  ),

  -- - Split the Transaction Code to extract the letters at the start of the transaction code.
  -- - Rename the values in the Online or In-person field, Online of the 1 values and In-Person for the 2 values.
  final AS (
    SELECT
      regexp_extract(transaction_code, '^([^-]+)-([^-]+)-([^-]+)-([^-]+)$', 1, 'i') AS bank_code,
      value,
      customer_code,
      CASE online_or_in_person
        WHEN 1 THEN 'Online'
        WHEN 2 THEN 'In-Person'
        ELSE 'Unknown'
      END AS online_or_in_person,
      try_cast(
        regexp_replace(
          transaction_date,
          '^(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/(\d{4}) ([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$',
          '\3-\2-\1',
          'g'
        ) AS date
      ) AS transaction_date
    FROM
      input_data
  )

SELECT * FROM final
