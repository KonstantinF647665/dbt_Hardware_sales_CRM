SELECT
    opportunity_id,
    close_value,
    deal_stage
FROM {{ source('raw_crm', 'sales_pipeline') }}
WHERE deal_stage NOT LIKE 'Won' and close_value > 0