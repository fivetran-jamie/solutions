SELECT
  date_trunc('DAY', time_stamp) AS date_day,
  json_extract_path_text(message_data, 'schema') as schema, 
  json_extract_path_text(message_data, 'table') as "table", 
  SUM(json_extract_path_text(message_data, 'count')::integer) as row_volume
FROM fivetran_log.log
WHERE 
  datediff(DAY, time_stamp, current_date) < 30
  AND message_event = 'records_modified'
group by date_day, schema, "table"
order by date_day desc;