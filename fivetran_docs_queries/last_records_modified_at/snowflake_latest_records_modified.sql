WITH parse_json AS (
    SELECT
        time_stamp,
        PARSE_JSON(message_data) AS message_data,
        message_event,
        MAX(CASE WHEN message_event = 'sync_end' THEN time_stamp ELSE NULL END) OVER(PARTITION BY connector_id) AS last_sync_completed_at
    FROM fivetran_log.log
    WHERE message_event = 'records_modified'
        OR message_event = 'sync_end'
)

select 
    message_data:schema as connector_schema,
    MAX(time_stamp) AS last_records_modified_at,
    SUM(message_data:count::integer) AS row_volume
FROM parse_json 
WHERE message_event = 'records_modified'
    AND time_stamp > last_sync_completed_at
GROUP BY connector_schema
ORDER BY last_records_modified_at DESC
;