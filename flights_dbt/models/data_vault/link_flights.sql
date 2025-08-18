{{ config(materialized='table') }}

with source as (
    select distinct
        flight_id,
        departure_airport,
        arrival_airport
    from {{ source('flights_staging', 'flights') }}
)

select distinct
    md5(
        cast(flight_id as varchar) || 
        cast(departure_airport as varchar) || 
        cast(arrival_airport as varchar)
    ) as flight_pk,
    md5(cast(departure_airport as varchar)) as departure_airport_pk,
    md5(cast(arrival_airport as varchar)) as arrival_airport_pk,
    current_timestamp as load_dts
from source