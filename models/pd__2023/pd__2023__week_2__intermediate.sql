WITH
  -- - Input the data: Swift codes
  input_data_swift_codes AS (
    SELECT
      *
    FROM
      {{ ref("pd__2023__week_2__input_1") }}
  ),

  -- - Input the data: Transactions
  input_data_transactions AS (
    SELECT
      *
    FROM
      {{ ref("pd__2023__week_2__input_2") }}
  ),

-- - In the Transactions table, there is a Sort Code field which contains dashes. We need to remove these so just have a 6 digit string
-- - Use the SWIFT Bank Code lookup table to bring in additional information about the SWIFT code and Check Digits of the receiving bank account
-- - Add a field for the Country Code (hint)
--     - Hint: all these transactions take place in the UK so the Country Code should be GB
-- - Create the IBAN as above (hint)
--     - Hint: watch out for trying to combine sting fields with numeric fields - check data types
  final AS (
    SELECT
      transactions.transaction_id,
      transactions.bank AS bank_name,
      'GB' AS country_code,
      swift_codes.check_digits,
      swift_codes.swift_code AS bank_code,
      regexp_replace(transactions.sort_code, '-', '', 'g') AS sort_code,
      transactions.account_number,
    FROM
      input_data_transactions AS transactions
    LEFT JOIN
      input_data_swift_codes AS swift_codes
    ON
      transactions.bank = swift_codes.bank
  )

SELECT * FROM final
