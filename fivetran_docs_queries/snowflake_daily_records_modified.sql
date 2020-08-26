WITH parse_json AS (
    SELECT
      date_trunc('DAY', time_stamp) AS date_day,
      PARSE_JSON(message_data) as message_data
    FROM fivetran_log.log
    WHERE datediff(DAY, time_stamp, current_date) < 30
      AND message_event = 'records_modified'
)

  select 
    date_day,
    message_data:schema as "schema",
    message_data:table as "table",
    sum(message_data:count::integer) as records_updated_or_inserted
  from parse_json
  where message_data:operationType = 'REPLACED_OR_INSERTED'
  group by date_day, "schema", "table"
  order by date_day desc;