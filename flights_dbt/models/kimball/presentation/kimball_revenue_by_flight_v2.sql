{{ config(materialized='table') }}

WITH flight_revenue AS (
    SELECT 
        flight_id,
        COUNT(ticket_no) AS tickets_sold,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_ticket_price,
        COUNT(CASE WHEN fare_conditions = 'Economy' THEN 1 END) AS economy_tickets,
        COUNT(CASE WHEN fare_conditions = 'Business' THEN 1 END) AS business_tickets,
        SUM(CASE WHEN fare_conditions = 'Economy' THEN amount ELSE 0 END) AS economy_revenue,
        SUM(CASE WHEN fare_conditions = 'Business' THEN amount ELSE 0 END) AS business_revenue
    FROM {{ ref('fct_tickets') }}
    GROUP BY flight_id
)

SELECT 
    f.flight_id,
    f.flight_no,
    f.scheduled_departure,
    dep_a.city as departure_city,
    arr_a.city as arrival_city,
    f.status,
    
    -- Revenue metrics from ticket fact
    COALESCE(fr.tickets_sold, 0) as tickets_sold,
    COALESCE(fr.total_revenue, 0) as total_revenue,
    COALESCE(fr.avg_ticket_price, 0) as avg_ticket_price,
    COALESCE(fr.economy_tickets, 0) as economy_tickets,
    COALESCE(fr.business_tickets, 0) as business_tickets,
    COALESCE(fr.economy_revenue, 0) as economy_revenue,
    COALESCE(fr.business_revenue, 0) as business_revenue

FROM {{ ref('fct_flights') }} f
LEFT JOIN flight_revenue fr ON f.flight_id = fr.flight_id
LEFT JOIN {{ ref('dim_airports') }} dep_a ON f.departure_airport_sk = dep_a.airport_sk
LEFT JOIN {{ ref('dim_airports') }} arr_a ON f.arrival_airport_sk = arr_a.airport_sk

-- This works! But notice the complex CTE and cross-fact joins