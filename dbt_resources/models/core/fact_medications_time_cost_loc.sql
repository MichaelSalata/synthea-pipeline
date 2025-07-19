{{
    config(
        persist_docs={"relation": true, "columns": true},
        materialized='incremental',
        unique_key='medication_id',
        on_schema_change='fail'
    )
}}

WITH organizations AS (
    SELECT *
    FROM {{ ref('stg_organizations_data') }} as o
),
encounters AS (
    SELECT *
    FROM {{ ref('stg_encounters_data') }} as e
),
enc_org_prov_pay AS (
    SELECT 
        -- Encounter fields
        encounters.encounter_id,
        encounters.patient_id,
        encounters.organization_id,
        encounters.provider_id,
        encounters.payer_id,
        encounters.encounter_class,
        encounters.encounter_code,
        encounters.encounter_description,
        encounters.reason_code,
        encounters.reason_description,
        encounters.start_timestamp,
        encounters.stop_timestamp,
        encounters.base_encounter_cost,
        encounters.total_claim_cost,
        encounters.payer_coverage,
        
        -- Organization fields (with aliases to avoid conflicts)
        organizations.organization_name,
        organizations.address,
        organizations.city,
        organizations.state,
        organizations.zip_code,
        organizations.phone,
        organizations.latitude,
        organizations.longitude,
        organizations.revenue,
        organizations.utilization
    FROM encounters
    LEFT JOIN organizations ON encounters.organization_id = organizations.organization_id
    -- LEFT JOIN payers ON encounters.payer_id = payers.payer_id
    -- LEFT JOIN providers ON encounters.provider_id = providers.provider_id
),
patients AS (
    SELECT *
    FROM {{ ref('stg_patients_data') }} as p
),
medications AS (
    SELECT *
    FROM {{ ref('stg_medications_data') }} as m
)

SELECT 
    -- MEDICATIONS
    -- primary key for fact table
    meds.medication_id,

    -- medication details
    meds.medication_code,
    meds.medication_description,
    meds.reason_code,
    meds.reason_description,

    -- timeline
    meds.start_timestamp,
    meds.stop_timestamp,
    meds.dispenses,

    -- cost information
    meds.base_cost,
    meds.payer_coverage,
    meds.total_cost,



    -- PATIENTS
    pats.patient_id,

    -- dates
    pats.birth_date,
    pats.death_date,

    -- demographics
    pats.marital_status,
    pats.race,
    pats.ethnicity,
    pats.gender,

    -- location
    pats.birthplace,
    pats.address,
    pats.city,
    pats.state,
    pats.county,
    pats.fips_code,
    pats.zip_code,

    -- coordinates
    pats.latitude,
    pats.longitude,

    -- financial metrics
    pats.healthcare_expenses,
    pats.healthcare_coverage,
    pats.income,



    -- ENCOUNTERS
    enc_org_prov_pay.encounter_id,

    -- encounter details
    enc_org_prov_pay.encounter_class,
    enc_org_prov_pay.encounter_code,
    enc_org_prov_pay.encounter_description,
    enc_org_prov_pay.reason_code as encounter_reason_code,
    enc_org_prov_pay.reason_description as encounter_reason_description,

    -- timeline
    enc_org_prov_pay.start_timestamp as encounter_start_timestamp,
    enc_org_prov_pay.stop_timestamp as encounter_stop_timestamp,

    -- cost information
    enc_org_prov_pay.base_encounter_cost,
    enc_org_prov_pay.total_claim_cost,
    enc_org_prov_pay.payer_coverage as encounter_payer_coverage,


    -- ORGANIZATIONS
    -- primary key
    enc_org_prov_pay.organization_id,

    -- organization details
    enc_org_prov_pay.organization_name,
    enc_org_prov_pay.address as organization_address,
    enc_org_prov_pay.city as organization_city,
    enc_org_prov_pay.state as organization_state,
    enc_org_prov_pay.zip_code as organization_zip_code,
    enc_org_prov_pay.phone,

    -- location coordinates
    enc_org_prov_pay.latitude as organization_latitude,
    enc_org_prov_pay.longitude as organization_longitude,

    -- financial metrics
    enc_org_prov_pay.revenue,
    enc_org_prov_pay.utilization


FROM medications meds
LEFT JOIN patients pats ON meds.patient_id = pats.patient_id
LEFT JOIN enc_org_prov_pay ON meds.encounter_id = enc_org_prov_pay.encounter_id

{% if is_incremental() %}
  -- Only process records that are newer than the latest record in the target table
  WHERE meds.start_timestamp > (SELECT MAX(start_timestamp) FROM {{ this }})
{% endif %}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}