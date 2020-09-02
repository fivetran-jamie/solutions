with order_logs as (
    SELECT
        time_stamp,
        json_extract_path_text(message_data, 'schema') AS connector_schema, 
        json_extract_path_text(message_data, 'count')::integer AS row_volume,
        ROW_NUMBER() OVER(PARTITION BY json_extract_path_text(message_data, 'schema') ORDER BY time_stamp DESC) AS nth_last_record
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