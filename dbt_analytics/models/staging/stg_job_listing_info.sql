{{ config(materialized="view", target_schema="jobs") }}

with stg_job_listing_info as (select * from {{ source("staging", "stg_job_listing") }})

select
    title,
    company,
    --convert hour payment to year
    case when avg_salary < 200 then avg_salary * 2080 else avg_salary end as avg_salary,
    city,
    state
from stg_job_listing_info

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
