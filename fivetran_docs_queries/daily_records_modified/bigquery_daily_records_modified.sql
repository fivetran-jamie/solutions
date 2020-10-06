SELECT
  DATE_TRUNC(cast(time_stamp AS date), day) AS date_day,
  JSON_EXTRACT(message_data, '$.schema') AS schema, 
  JSON_EXTRACT(message_data, '$.table') AS table, 
  SUM(cast(JSON_EXTRACT(message_data, '$.count') AS int64)) AS row_volume
FROM fivetran_log.log
WHERE DATE_DIFF(cast(CURRENT_DATE() AS date), cast(time_stamp AS date), DAY) < 30
  AND message_event = 'records_modified'
GROUP BY date_day, schema, table
ORDER BY date_day DESC;