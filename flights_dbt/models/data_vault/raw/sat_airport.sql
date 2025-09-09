{{ config(materialized='table') }}

with source as (
    select distinct
      airport_code,
      airport_name,
      city,
      cast(coordinates as varchar) as coordinates,
      timezone
    from {{ source('flights_staging', 'airports_data') }}
)

select
    md5(cast(airport_code as varchar)) as airport_pk,
    airport_name,
    city,
    cast(coordinates as point) as coordinates,
    timezone,
    current_timestamp as load_dts,
    'flights_staging.airports_data' as record_source
from source