SELECT 
COALESCE(NULLIF(NULLIF(parent_account, '0'), ''), account_name) AS ultimate_parent_name,
    industry,
    year_established,
    annual_revenue,
    employee_count,
    office_location FROM {{ ref('stg_accounts') }}