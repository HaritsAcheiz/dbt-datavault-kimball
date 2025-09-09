{{ config(materialized='table') }}

WITH flight_details AS (
    SELECT 
        hf.flight_pk,
        sf.flight_no,
        hf.flight_id,
        sf.scheduled_departure,
        sf.status
    FROM {{ ref('hub_flights') }} hf
    LEFT JOIN {{ ref('sat_flights') }} sf ON hf.flight_pk = sf.flight_pk
),

ticket_summary AS (
    SELECT 
        ltf.flight_pk,
        COUNT(ht.ticket_no) AS tickets_sold,
        SUM(st.amount) AS total_revenue,
        AVG(st.amount) AS avg_ticket_price,
        COUNT(CASE WHEN st.fare_conditions = 'Economy' THEN 1 END) AS economy_tickets,
        COUNT(CASE WHEN st.fare_conditions = 'Business' THEN 1 END) AS business_tickets,
        SUM(CASE WHEN st.fare_conditions = 'Economy' THEN st.amount ELSE 0 END) AS economy_revenue,
        SUM(CASE WHEN st.fare_conditions = 'Business' THEN st.amount ELSE 0 END) AS business_revenue
    FROM {{ ref('link_ticket_flights') }} ltf
    LEFT JOIN {{ ref('hub_tickets') }} ht ON ltf.ticket_pk = ht.ticket_pk
    LEFT JOIN {{ ref('sat_tickets') }} st ON ht.ticket_flight_pk = st.ticket_flight_pk
    GROUP BY ltf.flight_pk
),

-- Get airport details by joining Hub and Satellite
airport_details AS (
    SELECT
        ha.airport_pk,
        sa.city
    FROM {{ ref('hub_airport') }} ha
    LEFT JOIN {{ ref('sat_airport') }} sa ON ha.airport_pk = sa.airport_pk
),

-- Join flight details with airport link table twice to get departure and arrival cities
flight_airport_details AS (
    SELECT
        lfa.flight_pk,
        dep_airport.city AS departure_city,
        arr_airport.city AS arrival_city
    FROM {{ ref('link_flights_airport') }} lfa
    LEFT JOIN airport_details AS dep_airport ON lfa.departure_airport_pk = dep_airport.airport_pk
    LEFT JOIN airport_details AS arr_airport ON lfa.arrival_airport_pk = arr_airport.airport_pk
)

SELECT 
    fd.flight_id,
    fd.flight_no,
    fd.scheduled_departure,
    fad.departure_city,
    fad.arrival_city,
    fd.status,

    -- Revenue metrics from ticket summary
    COALESCE(ts.tickets_sold, 0) as tickets_sold,
    COALESCE(ts.total_revenue, 0) as total_revenue,
    COALESCE(ts.avg_ticket_price, 0) as avg_ticket_price,
    COALESCE(ts.economy_tickets, 0) as economy_tickets,
    COALESCE(ts.business_tickets, 0) as business_tickets,
    COALESCE(ts.economy_revenue, 0) as economy_revenue,
    COALESCE(ts.business_revenue, 0) as business_revenue
FROM flight_details fd
LEFT JOIN ticket_summary ts ON fd.flight_pk = ts.flight_pk
LEFT JOIN flight_airport_details fad ON fd.flight_pk = fad.flight_pk