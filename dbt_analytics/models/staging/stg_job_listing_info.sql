{{ config(materialized="view") }}

with stg_job_listing_info as (select * from {{ source("staging", "stg_job_listing") }})

select
    title,
    company,
    -- convert hour payment to year
    --handle outliers
    case
        when avg_salary between 20 and 200
        then avg_salary * 2080
        when avg_salary between 40000 and 700000
        then avg_salary
        else null
    end as avg_salary,
    city,
    state
from stg_job_listing_info

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
