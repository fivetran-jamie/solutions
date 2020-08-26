SELECT
  date_trunc(cast(time_stamp as date), day) AS date_day,
  JSON_EXTRACT(message_data, '$.schema') as schema, 
  JSON_EXTRACT(message_data, '$.table') as table, 
  SUM(cast(JSON_EXTRACT(message_data, '$.count') as int64)) as records_updated_or_inserted
FROM fivetran_log.log
WHERE date_diff( cast(CURRENT_DATE() as date), cast(time_stamp as date), DAY) < 30
  AND message_event = 'records_modified'
  and JSON_EXTRACT_SCALAR(message_data, '$.operationType') = 'REPLACED_OR_INSERTED'
group by date_day, schema, table
order by date_day desc
