{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        ticket_no,
        flight_id,
        fare_conditions,
        amount
    FROM {{ source('flights_staging', 'ticket_flights') }}
)

select
    md5(
        cast(ticket_no as varchar) ||
        cast(flight_id as varchar)
    ) as ticket_flight_pk,
    fare_conditions,
    amount,
    current_timestamp as load_dts,
    'flights_staging.ticket_flights' as record_source
from source