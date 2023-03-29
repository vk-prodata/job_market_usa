{{ config(materialized='table') }}

with job_listing as (
    select * from {{ ref('stg_job_listing_info') }}
),

dim_job_title as (
    select * from {{ ref('dim_job_title') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
),

dim_company as (
    select * from {{ ref('dim_company') }}
)

select 
    dl.loc_id,
    dc.company_id,
    dt.title_id,
    jl.avg_salary

from job_listing jl
inner join dim_job_title as dt
on jl.title = dt.title
inner join dim_location as dl
on jl.city = dl.city and jl.state = dl.state
inner join dim_company as dc
on jl.company = dc.company
