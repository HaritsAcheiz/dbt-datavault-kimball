-- REBUILT VERSION - Ticket grain (one record per ticket!)
{{ config(materialized='table') }}

WITH tickets AS (
    SELECT * FROM {{ source('flights_staging', 'ticket_flights') }}
),

flights AS (
    SELECT * FROM {{ source('flights_staging', 'flights') }}
),

airports AS (
    SELECT * FROM {{ ref('dim_airports') }}
)

SELECT
    t.ticket_no,                            -- primary key Was flight_id before!
    t.flight_id,                            -- Now a foreign key, not primary
    f.flight_no,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.status,
    f.aircraft_code,
    f.actual_departure,
    f.actual_arrival,
    dep_airport.airport_sk AS departure_airport_sk,
    arr_airport.airport_sk AS arrival_airport_sk,
    -- f.aircraft_type,
    -- f.total_seats,                          -- Now duplicated per ticket!
    -- t.customer_id,                          -- New ticket-level data
    t.fare_conditions,                         -- t.seat_class,
    -- t.booking_time,
    t.amount                                   -- t.ticket_price,
    -- t.sales_channel,
    -- DATEDIFF('minute', f.departure_time, f.arrival_time) as flight_duration_minutes
FROM tickets t
JOIN flights f ON t.flight_id = f.flight_id
JOIN airports AS dep_airport
    ON f.departure_airport = dep_airport.airport_code
JOIN airports AS arr_airport
    ON f.arrival_airport = arr_airport.airport_code
