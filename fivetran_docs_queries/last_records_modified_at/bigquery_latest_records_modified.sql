with order_logs as (
    SELECT
        time_stamp,
        JSON_EXTRACT(message_data, '$.schema') AS connector_schema, 
        CAST(JSON_EXTRACT(message_data, '$.count') AS int64) AS row_volume,
        ROW_NUMBER() OVER(PARTITION BY JSON_EXTRACT(message_data, '$.schema') ORDER BY time_stamp DESC) AS nth_last_record
    FROM fivetran_log.log
    WHERE message_event = 'records_modified'
)

select 
    time_stamp AS last_records_modified_at,
    connector_schema,
    SUM(row_volume) AS row_volume
FROM order_logs
WHERE nth_last_record = 1
GROUP BY last_records_modified_at, connector_schema
ORDER BY last_records_modified_at DESC
;