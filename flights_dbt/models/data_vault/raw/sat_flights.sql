{{ config(materialized='table') }}

with source as (
  select distinct
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival
  from
    {{ source('flights_staging', 'flights') }}
)

select
    md5(cast(flight_id as varchar)) as flight_pk,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival, 
    current_timestamp as load_dts,
    'flights_staging.flights' as record_source
from source