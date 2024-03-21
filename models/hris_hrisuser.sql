{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert',
)}}
SELECT
    DISTINCT {{ var("table_prefix") }}_employees.id as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
        '{{ var("integration_id") }}' || {{ var("table_prefix") }}_employees.id || 'user' || 'factorial'
    ) as id,
    'factorial' as source,
    '{{ var("integration_id") }}' :: uuid as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_employees._airbyte_data as last_raw_data,
    '{{ var("timestamp") }}' as sync_timestamp,
    {{ var("table_prefix") }}_employees.email as email,
    {{ var("table_prefix") }}_employees.first_name as firstname,
    {{ var("table_prefix") }}_employees.birthday_on as birth_date,
    {{ var("table_prefix") }}_employees.last_name as lastname,
    {{ var("table_prefix") }}_employees.contact_number as phone_number,
    NULL as language,
    legal_entity.legal_name as legal_entity_name,
    leader.email as manager_email,
    {{ var("table_prefix") }}_contracts.role as job_title,
    {{ var("table_prefix") }}_contracts.job_title as team,
    {{ var("table_prefix") }}_contracts.country as country,
    {{ var("table_prefix") }}_contracts.effective_on as contract_start_date,
    {{ var("table_prefix") }}_contracts.ends_on as contract_end_date,
    {{ var("table_prefix") }}_employees.gender as gender,
    {{ var("table_prefix") }}_contracts.job_title as business_unit,
    NULL as employee_number,
    {{ var("table_prefix") }}_contracts.fr_employee_type as contract_type,
    {{ var("table_prefix") }}_contracts.job_title as socio_professional_category,
    {{ var("table_prefix") }}_employees.state as state
FROM
    {{ var("table_prefix") }}_employees
    LEFT JOIN {{ var("table_prefix") }}_contracts ON {{ var("table_prefix") }}_employees.id = {{ var("table_prefix") }}_contracts.employee_id
    LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_employees ON {{ var("table_prefix") }}_employees._airbyte_ab_id = "_airbyte_raw_{{ var("table_prefix") }}_employees"._airbyte_ab_id
    LEFT JOIN (
        SELECT
            MAX(description) as description,
            MAX(name) as name,
            employee_id,
            MAX(lead_id) as lead_id
        FROM
(
                SELECT
                    company_id,
                    description,
                    name,
                    jsonb_array_elements_text(lead_ids) :: INT AS lead_id
                FROM
                    {{ var("table_prefix") }}_teams
            ) as employee_list NATURAL
            LEFT JOIN (
                SELECT
                    name,
                    jsonb_array_elements_text(employee_ids) :: INT AS employee_id
                FROM
                    {{ var("table_prefix") }}_teams
            ) as team_list
        GROUP BY
            employee_id
    ) as leads on leads.employee_id = {{ var("table_prefix") }}_employees.id
    LEFT JOIN {{ var("table_prefix") }}_employees as leader ON leads.lead_id = leader.id
    LEFT JOIN {{ var("table_prefix") }}_legal_entities as legal_entity ON legal_entity.id = {{ var("table_prefix") }}_employees.legal_entity_id