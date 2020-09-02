WITH parse_json AS (
    SELECT
        time_stamp,
        PARSE_JSON(message_data) AS message_data
    FROM fivetran_log.log
    WHERE message_event = 'records_modified'
)

SELECT 
    time_stamp AS last_records_modified_at,
    message_data:schema AS connector_schema,
    SUM(message_data:count::integer) AS row_volume
FROM parse_json
GROUP BY time_stamp, connector_schema
QUALIFY ROW_NUMBER() OVER(PARTITION BY message_data:schema ORDER BY time_stamp DESC) = 1
;