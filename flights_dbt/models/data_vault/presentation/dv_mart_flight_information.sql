{{ config(materialized='table') }}

select
    -- Flight ID from the Link table
    h_f.flight_id,
    -- Flight details from the Satellites
    s_f.flight_no,
    -- Departure Airport details from Hub and Satellite
    h_dep.airport_code as departure_airport_code,
    s_dep.airport_name as departure_airport_name,
    s_dep.city as departure_city,
    -- Arrival Airport details from Hub and Satellite
    h_arr.airport_code as arrival_airport_code,
    s_arr.airport_name as arrival_airport_name,
    s_arr.city as arrival_city,
    EXTRACT(EPOCH FROM (s_f.scheduled_arrival - s_f.scheduled_departure)) / 60 as scheduled_duration_minutes
from {{ ref('link_flights_airport') }} l
left join {{ ref('hub_flights') }} h_f
    on l.flight_pk = h_f.flight_pk
left join {{ ref('sat_flights') }} s_f
    on h_f.flight_pk = s_f.flight_pk
left join {{ ref('hub_airport') }} h_dep
    on l.departure_airport_pk = h_dep.airport_pk
left join {{ ref('sat_airport') }} s_dep
    on h_dep.airport_pk = s_dep.airport_pk
left join {{ ref('hub_airport') }} h_arr
    on l.arrival_airport_pk = h_arr.airport_pk
left join {{ ref('sat_airport') }} s_arr
    on h_arr.airport_pk = s_arr.airport_pk