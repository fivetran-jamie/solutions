WITH parse_json AS (
  SELECT
    DATE_TRUNC('DAY', time_stamp) AS date_day,
    PARSE_JSON(message_data) AS message_data
  FROM fivetran_log.log
  WHERE DATEDIFF(DAY, time_stamp, current_date) < 30
    AND message_event = 'records_modified'
)

SELECT 
  date_day,
  message_data:schema AS "schema",
  message_data:table AS "table",
  SUM(message_data:count::integer) AS row_volume
FROM parse_json
GROUP BY date_day, "schema", "table"
ORDER BY date_day DESC;