-- Create table for non-Flow Card holders
SELECT
  *
FROM
  {{ ref("pd__2024__week_1__intermediate") }}
WHERE
  flow_card = 'No'
