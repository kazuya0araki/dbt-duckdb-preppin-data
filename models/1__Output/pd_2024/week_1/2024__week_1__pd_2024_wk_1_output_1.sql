-- Create table for Flow Card holders
SELECT
  *
FROM
  {{ ref("2024__week_1__pd_2024_wk_1_output_intermediate") }}
WHERE
  flow_card = 'Yes'
