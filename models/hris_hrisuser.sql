{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert',
) }}
SELECT
    DISTINCT _airbyte_raw_{{var("table_prefix")}}_users._airbyte_data->>'id' as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
        '{{ var("integration_id") }}'::text  || (_airbyte_data->>'id')::text || 'user' || 'kiwihr'
    ) as id,
    'kiwihr' as source,
    '{{ var("integration_id") }}' :: uuid as integration_id,
    _airbyte_raw_{{var("table_prefix")}}_users._airbyte_data as last_raw_data,
    '{{ var("timestamp") }}' as sync_timestamp,
    "_airbyte_data" ->> 'firstName' as "firstname",
    "_airbyte_data" ->> 'lastName' as "lastname",
    "_airbyte_data" ->> 'email' as "email",
    "_airbyte_data" ->> 'birthDate' as "birth_date",
    "_airbyte_data" ->> 'personalPhone' as "phone_number",
    "_airbyte_data" ->> 'language' as "language",
    "_airbyte_data" -> 'team' ->> 'name' as "legal_entity_name",
    "_airbyte_data" -> 'manager' ->> 'email' as "manager_email",
    "_airbyte_data" -> 'position' ->> 'name' as "job_title",
    "_airbyte_data" -> 'team' ->> 'name' as "team",
    "_airbyte_data" ->> 'employmentStartDate' as "contract_start_date",
    "_airbyte_data" ->> 'employmentEndDate' as "contract_end_date",
    "_airbyte_data" ->> 'gender' as "gender",
    "_airbyte_data" -> 'team' ->> 'name' as "business_unit",
    "_airbyte_data" ->> 'employeeNumber' as "employee_number",
    "_airbyte_data" -> 'contract' ->> 'name' as "contract_type",
    "_airbyte_data" ->> 'socio_professional_category' as "socio_professional_category",
    "_airbyte_data" ->> 'state' as "state",
    "_airbyte_data" -> 'location' ->> 'name' as "country"
FROM
    _airbyte_raw_{{var("table_prefix")}}_users