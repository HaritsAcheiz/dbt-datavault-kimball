{{ config(materialized='table') }}

select
   ticket_no,
   flight_id,
   fare_conditions,
   amount
from {{ source('flights_staging', 'ticket_flights') }}