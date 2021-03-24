with order_measurements as (
    select
        schema_name,
        table_name,
        destination_id,
        measured_at,
        to_char(measured_at, 'YYYY-MM') as measured_month,
        monthly_active_rows,
        row_number() over(partition by table_name, schema_name, destination_id, to_char(measured_at, 'YYYY-MM') order by measured_at desc) as nth_measurement
    from <fivetran_load_database>.<fivetran_log_schema_name>.active_volume 
)

select
    schema_name,
    destination_id,
    measured_month,
    sum(monthly_active_rows) as MAR
from order_measurements
where nth_measurement = 1 
group by schema_name, destination_id, measured_month
order by measured_month, schema_name