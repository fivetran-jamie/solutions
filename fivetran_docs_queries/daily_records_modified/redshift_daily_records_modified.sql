SELECT
  DATE_TRUNC('DAY', time_stamp) AS date_day,
  JSON_EXTRACT_PATH_TEXT(message_data, 'schema') AS schema, 
  JSON_EXTRACT_PATH_TEXT(message_data, 'table') AS "table", 
  SUM(JSON_EXTRACT_PATH_TEXT(message_data, 'count')::integer) AS row_volume
FROM fivetran_log.log
WHERE 
  DATEDIFF(day, time_stamp, current_date) < 30
  AND message_event = 'records_modified'
GROUP BY date_day, schema, "table"
ORDER BY date_day DESC;