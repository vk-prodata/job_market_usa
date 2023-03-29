{{ config(materialized='table') }}

with job_listing as (
    select * from {{ ref('stg_job_listing_info') }}
)
select distinct
    {{ dbt_utils.generate_surrogate_key(['city', 'state']) }} as loc_id,
    city,
    state
from job_listing