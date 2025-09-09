{{ config(materialized='table') }}

with source as (
    select distinct
      flight_id
    from
      {{ source('flights_staging', 'flights') }}
)

select
    md5(cast(flight_id as varchar)) as flight_pk,
    flight_id,
    current_timestamp as load_dts,
    'flights_staging.flights' as record_source
from source