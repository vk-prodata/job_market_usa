{{ config(materialized='table') }}

with fact_job as (
    select * from {{ ref('fact_job') }}
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
    dl.city,
    dl.state,
    dc.company,
    dt.title,
    jl.avg_salary

from fact_job jl
inner join dim_job_title as dt
on jl.title_id = dt.title_id
inner join dim_location as dl
on jl.loc_id = dl.loc_id
inner join dim_company as dc
on jl.company_id = dc.company_id
