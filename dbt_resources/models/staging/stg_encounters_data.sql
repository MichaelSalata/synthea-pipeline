{{ config(materialized='view') }}

with encounters_data as 
(
  select *,
    row_number() over(partition by Id) as duplicate_counter
  from {{ source('staging','encounters_raw') }}
  where Id is not null
)

select
    -- primary key
    {{ dbt.safe_cast("Id", api.Column.translate_type("string")) }} as encounter_id,

    -- identifiers (foreign keys)
    {{ dbt.safe_cast("PATIENT", api.Column.translate_type("string")) }} as patient_id,
    {{ dbt.safe_cast("ORGANIZATION", api.Column.translate_type("string")) }} as organization_id,
    {{ dbt.safe_cast("PROVIDER", api.Column.translate_type("string")) }} as provider_id,
    {{ dbt.safe_cast("PAYER", api.Column.translate_type("string")) }} as payer_id,

    -- encounter details
    {{ dbt.safe_cast("ENCOUNTERCLASS", api.Column.translate_type("string")) }} as encounter_class,
    {{ dbt.safe_cast("CODE", api.Column.translate_type("string")) }} as encounter_code,
    {{ dbt.safe_cast("DESCRIPTION", api.Column.translate_type("string")) }} as encounter_description,
    {{ dbt.safe_cast("REASONCODE", api.Column.translate_type("string")) }} as reason_code,
    {{ dbt.safe_cast("REASONDESCRIPTION", api.Column.translate_type("string")) }} as reason_description,

    -- timeline
    {{ dbt.safe_cast("START", api.Column.translate_type("timestamp")) }} as start_timestamp,
    {{ dbt.safe_cast("STOP", api.Column.translate_type("timestamp")) }} as stop_timestamp,

    -- cost information
    {{ dbt.safe_cast("BASE_ENCOUNTER_COST", api.Column.translate_type("numeric")) }} as base_encounter_cost,
    {{ dbt.safe_cast("TOTAL_CLAIM_COST", api.Column.translate_type("numeric")) }} as total_claim_cost,
    {{ dbt.safe_cast("PAYER_COVERAGE", api.Column.translate_type("numeric")) }} as payer_coverage

from encounters_data
where duplicate_counter = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
