{{ config(materialized='view') }}

with patients_data as 
(
  select *,
    row_number() over(partition by Id) as duplicate_counter
  from {{ source('staging','patients_raw') }}
  where Id is not null
)

select
    -- primary key
    {{ dbt.safe_cast("Id", api.Column.translate_type("string")) }} as patient_id,

    -- dates
    {{ dbt.safe_cast("BIRTHDATE", api.Column.translate_type("date")) }} as birth_date,
    {{ dbt.safe_cast("DEATHDATE", api.Column.translate_type("date")) }} as death_date,

    -- identification
    {{ dbt.safe_cast("SSN", api.Column.translate_type("string")) }} as ssn,
    {{ dbt.safe_cast("DRIVERS", api.Column.translate_type("string")) }} as drivers_license,
    {{ dbt.safe_cast("PASSPORT", api.Column.translate_type("string")) }} as passport,

    -- name fields
    {{ dbt.safe_cast("PREFIX", api.Column.translate_type("string")) }} as name_prefix,
    {{ dbt.safe_cast("FIRST", api.Column.translate_type("string")) }} as first_name,
    {{ dbt.safe_cast("MIDDLE", api.Column.translate_type("string")) }} as middle_name,
    {{ dbt.safe_cast("LAST", api.Column.translate_type("string")) }} as last_name,
    {{ dbt.safe_cast("SUFFIX", api.Column.translate_type("string")) }} as name_suffix,
    {{ dbt.safe_cast("MAIDEN", api.Column.translate_type("string")) }} as maiden_name,

    -- demographics
    {{ dbt.safe_cast("MARITAL", api.Column.translate_type("string")) }} as marital_status,
    {{ dbt.safe_cast("RACE", api.Column.translate_type("string")) }} as race,
    {{ dbt.safe_cast("ETHNICITY", api.Column.translate_type("string")) }} as ethnicity,
    {{ dbt.safe_cast("GENDER", api.Column.translate_type("string")) }} as gender,

    -- location
    {{ dbt.safe_cast("BIRTHPLACE", api.Column.translate_type("string")) }} as birthplace,
    {{ dbt.safe_cast("ADDRESS", api.Column.translate_type("string")) }} as address,
    {{ dbt.safe_cast("CITY", api.Column.translate_type("string")) }} as city,
    {{ dbt.safe_cast("STATE", api.Column.translate_type("string")) }} as state,
    {{ dbt.safe_cast("COUNTY", api.Column.translate_type("string")) }} as county,
    {{ dbt.safe_cast("FIPS", api.Column.translate_type("string")) }} as fips_code,
    {{ dbt.safe_cast("ZIP", api.Column.translate_type("string")) }} as zip_code,

    -- coordinates
    {{ dbt.safe_cast("LAT", api.Column.translate_type("float64")) }} as latitude,
    {{ dbt.safe_cast("LON", api.Column.translate_type("float64")) }} as longitude,

    -- financial metrics
    {{ dbt.safe_cast("HEALTHCARE_EXPENSES", api.Column.translate_type("numeric")) }} as healthcare_expenses,
    {{ dbt.safe_cast("HEALTHCARE_COVERAGE", api.Column.translate_type("numeric")) }} as healthcare_coverage,
    {{ dbt.safe_cast("INCOME", api.Column.translate_type("numeric")) }} as income

from patients_data
where duplicate_counter = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
