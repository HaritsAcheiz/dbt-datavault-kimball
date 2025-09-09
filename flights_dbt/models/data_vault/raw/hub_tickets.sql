{{ config(materialized='table') }}

with source as (
    select distinct
      ticket_no,
      flight_id
    from
      {{ source('flights_staging', 'ticket_flights') }}
)

select
    md5(
      cast(ticket_no as varchar) ||
      cast(flight_id as varchar)
    ) as ticket_flight_pk,
    md5( cast(ticket_no as varchar) ) as ticket_pk,
    md5( cast(flight_id as varchar) ) as flight_pk,
    ticket_no,
    flight_id,
    current_timestamp as load_dts,
    'flights_staging.ticket_flights' as record_source
from source