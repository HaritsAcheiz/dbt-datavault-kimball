{{ config(materialized='table') }}

with source as (
    select distinct airport_code from {{ source('flights_staging', 'airports_data') }}
)

select
    md5(cast(airport_code as varchar)) as airport_pk,
    airport_code as airport_nk,
    current_timestamp as load_dts
from source