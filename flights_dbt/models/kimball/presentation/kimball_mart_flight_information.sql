{{ config(materialized='table') }}

select
    f.flight_id,
    f.flight_no,
    -- Departure Airport Details
    dep_a.airport_code as departure_airport_code,
    dep_a.airport_name as departure_airport_name,
    dep_a.city as departure_city,
    -- Arrival Airport Details
    arr_a.airport_code as arrival_airport_code,
    arr_a.airport_name as arrival_airport_name,
    arr_a.city as arrival_city,
    -- Calculate duration
    EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60 as scheduled_duration_minutes
from {{ ref('fct_flights') }} f
left join {{ ref('dim_airports') }} dep_a
    on f.departure_airport_sk = dep_a.airport_sk
left join {{ ref('dim_airports') }} arr_a
    on f.arrival_airport_sk = arr_a.airport_sk
