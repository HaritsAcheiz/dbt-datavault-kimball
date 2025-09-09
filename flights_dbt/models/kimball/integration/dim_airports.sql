{{ config(materialized='table') }}

with
source as (
  select
    *
  from
  {{ source('flights_staging', 'airports_data') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['airport_code']) }} as airport_sk,
  airport_code,
  airport_name,
  city,
  coordinates,
  timezone
from
  source