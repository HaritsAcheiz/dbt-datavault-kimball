{{ config(materialized='table') }}

with flights as (
    select * from {{ source('flights_staging', 'flights') }}
),

airports as (
    select
        {{ dbt_utils.generate_surrogate_key(['airport_code']) }} as airport_sk,
        airport_code
    from {{ ref('dim_airports') }}
)

select
    f.flight_id,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.status,
    -- Join to the airport dimension to get surrogate keys
    dep_airport.airport_sk as departure_airport_sk,
    arr_airport.airport_sk as arrival_airport_sk
from flights as f
join airports as dep_airport
    on f.departure_airport = dep_airport.airport_code
join airports as arr_airport
    on f.arrival_airport = arr_airport.airport_code