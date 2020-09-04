with parse_json as (
    SELECT
        time_stamp,
        JSON_EXTRACT(message_data, '$.schema') AS connector_schema, 
        CAST(JSON_EXTRACT(message_data, '$.count') AS int64) AS row_volume,
        message_event,
        MAX(CASE WHEN message_event = 'sync_end' THEN time_stamp ELSE NULL END) OVER(PARTITION BY connector_id) AS last_sync_completed_at
    FROM fivetran_log.log
    WHERE message_event = 'records_modified'
        OR message_event = 'sync_end'
)

select 
    connector_schema,
    MAX(time_stamp) AS last_records_modified_at,
    SUM(CASE WHEN time_stamp > last_sync_completed_at or last_sync_completed_at is null THEN row_volume ELSE 0 END) AS row_volume_since_last_sync
FROM parse_json
WHERE message_event = 'records_modified'
GROUP BY connector_schema
ORDER BY row_volume_since_last_sync DESC
;