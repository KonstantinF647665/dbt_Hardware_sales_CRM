select TRIM(LOWER(opportunity_id)) deal_id,
LOWER(TRIM(sales_agent)) agent_name,
LOWER(REPLACE(product::text, ' ', '')) product_name,
COALESCE(NULLIF(LOWER(TRIM(account)), ''), '0') account_name,
CASE
WHEN TRIM(deal_stage) LIKE 'Won' THEN 'Сделка заключена'
WHEN TRIM(deal_stage) LIKE 'Engaging' THEN 'Работа с клиентом'
WHEN TRIM(deal_stage) LIKE 'Lost' THEN 'Сделка не состоялась'
WHEN TRIM(deal_stage) LIKE 'Prospecting' THEN 'Разведка'
ELSE 'Другое'
END deal_stage,
engage_date::date,
NULLIF(TRIM(close_date::text), ''):: date close_date,
COALESCE(close_value::numeric, 0) deal_value
from {{ source('raw_crm', 'sales_pipeline') }}