{{ config(materialized='view') }}

with organizations_data as 
(
  select *,
    row_number() over(partition by Id) as duplicate_counter
  from {{ source('staging','organizations_raw') }}
  where Id is not null
)

select
    -- primary key
    {{ dbt.safe_cast("Id", api.Column.translate_type("string")) }} as organization_id,

    -- organization details
    {{ dbt.safe_cast("NAME", api.Column.translate_type("string")) }} as organization_name,
    {{ dbt.safe_cast("ADDRESS", api.Column.translate_type("string")) }} as address,
    {{ dbt.safe_cast("CITY", api.Column.translate_type("string")) }} as city,
    {{ dbt.safe_cast("STATE", api.Column.translate_type("string")) }} as state,
    {{ dbt.safe_cast("ZIP", api.Column.translate_type("string")) }} as zip_code,
    {{ dbt.safe_cast("PHONE", api.Column.translate_type("string")) }} as phone,

    -- location coordinates
    {{ dbt.safe_cast("LAT", api.Column.translate_type("float64")) }} as latitude,
    {{ dbt.safe_cast("LON", api.Column.translate_type("float64")) }} as longitude,

    -- financial metrics
    {{ dbt.safe_cast("REVENUE", api.Column.translate_type("numeric")) }} as revenue,
    {{ dbt.safe_cast("UTILIZATION", api.Column.translate_type("integer")) }} as utilization

from organizations_data
where duplicate_counter = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
