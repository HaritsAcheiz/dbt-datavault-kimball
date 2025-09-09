{{ config(materialized='table') }}

SELECT 
    nf.flight_id,
    nf.flight_no,
    nf.scheduled_departure,
    dep_a.city as departure_city,
    arr_a.city as arrival_city,
    nf.status,
    
    -- Revenue metrics (aggregated from tickets)
    COUNT(nf.ticket_no) as tickets_sold,
    SUM(nf.amount) as total_revenue,
    AVG(nf.amount) as avg_ticket_price,
    
    -- Fare class breakdown
    COUNT(CASE WHEN nf.fare_conditions = 'Economy' THEN 1 END) as economy_tickets,
    COUNT(CASE WHEN nf.fare_conditions = 'Business' THEN 1 END) as business_tickets,
    
    -- Revenue by class
    SUM(CASE WHEN nf.fare_conditions = 'Economy' THEN nf.amount ELSE 0 END) as economy_revenue,
    SUM(CASE WHEN nf.fare_conditions = 'Business' THEN nf.amount ELSE 0 END) as business_revenue
    
FROM {{ ref('new_fct_flights') }} nf
LEFT JOIN {{ ref('dim_airports') }} dep_a ON nf.departure_airport_sk = dep_a.airport_sk  
LEFT JOIN {{ ref('dim_airports') }} arr_a ON nf.arrival_airport_sk = arr_a.airport_sk
GROUP BY 
    nf.flight_id, nf.flight_no, nf.scheduled_departure, 
    dep_a.city, arr_a.city, nf.status