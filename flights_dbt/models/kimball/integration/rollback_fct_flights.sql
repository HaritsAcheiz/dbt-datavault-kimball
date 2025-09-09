{{ config(materialized='view') }}

SELECT DISTINCT  -- Need DISTINCT to remove ticket duplicates!
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    departure_airport_sk,
    arrival_airport_sk
FROM {{ ref('new_fct_flights') }}