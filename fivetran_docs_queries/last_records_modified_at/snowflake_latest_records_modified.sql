WITH parse_json AS (
    SELECT
        time_stamp,
        PARSE_JSON(message_data) AS message_data
    FROM fivetran_log.log
    WHERE message_event = 'records_modified'
),

order_logs as (
    SELECT
        time_stamp,
        message_data:schema AS connector_schema, 
        message_data:count::integer AS row_volume,
        DENSE_RANK() OVER(PARTITION BY message_data:schema 
            ORDER BY DATE_TRUNC('MINUTE', time_stamp) DESC) AS nth_last_record
    FROM parse_json
)

select 
    connector_schema,
    MAX(time_stamp) AS last_records_modified_at,
    SUM(row_volume) AS row_volume
FROM order_logs
WHERE nth_last_record = 1
GROUP BY connector_schema
ORDER BY last_records_modified_at DESC
;