{{ config(materialized='view') }}

with medications_data as 
(
  select *,
    row_number() over(partition by PATIENT, CODE, ENCOUNTER) as duplicate_counter
  from {{ source('staging','medications_raw') }}
  where PATIENT is not null CODE is not null ENCOUNTER is not null
)

select
    -- generated key
    {{ dbt_utils.generate_surrogate_key(['PATIENT', 'CODE', 'ENCOUNTER']) }} as medication_id,

    -- identifiers (foreign keys)
    {{ dbt.safe_cast("PATIENT", api.Column.translate_type("string")) }} as patient_id,
    {{ dbt.safe_cast("PAYER", api.Column.translate_type("string")) }} as payer_id,
    {{ dbt.safe_cast("ENCOUNTER", api.Column.translate_type("string")) }} as encounter_id,

    -- medication details
    {{ dbt.safe_cast("CODE", api.Column.translate_type("string")) }} as medication_code,
    {{ dbt.safe_cast("DESCRIPTION", api.Column.translate_type("string")) }} as medication_description,
    {{ dbt.safe_cast("REASONCODE", api.Column.translate_type("string")) }} as reason_code,
    {{ dbt.safe_cast("REASONDESCRIPTION", api.Column.translate_type("string")) }} as reason_description,

    -- timeline
    {{ dbt.safe_cast("START", api.Column.translate_type("timestamp")) }} as start_timestamp,
    {{ dbt.safe_cast("STOP", api.Column.translate_type("timestamp")) }} as stop_timestamp,
    {{ dbt.safe_cast("DISPENSES", api.Column.translate_type("integer")) }} as dispenses,

    -- cost information
    {{ dbt.safe_cast("BASE_COST", api.Column.translate_type("numeric")) }} as base_cost,
    {{ dbt.safe_cast("PAYER_COVERAGE", api.Column.translate_type("numeric")) }} as payer_coverage,
    {{ dbt.safe_cast("TOTALCOST", api.Column.translate_type("numeric")) }} as total_cost


from medications_data
where duplicate_counter = 1


-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}