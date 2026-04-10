select TRIM(LOWER(opportunity_id)) Идентификатор_сделки,
LOWER(TRIM(sales_agent)) Имя_агента,
LOWER(REPLACE(product::text, ' ', '')) Название_продукта,
COALESCE(NULLIF(LOWER(TRIM(account)), ''), '0') Название_компании,
CASE
WHEN TRIM(deal_stage) LIKE 'Won' THEN 'Сделка заключена'
WHEN TRIM(deal_stage) LIKE 'Engaging' THEN 'Работа с клиентом'
WHEN TRIM(deal_stage) LIKE 'Lost' THEN 'Сделка не состоялась'
WHEN TRIM(deal_stage) LIKE 'Prospecting' THEN 'Разведка'
ELSE 'Другое'
END Статус_сделки,
engage_date::date Активная_стадия_с,
NULLIF(TRIM(close_date::text), ''):: date Работа_по_сделке_окончена,
COALESCE(close_value::numeric, 0) Цена_сделки
from {{ ref('sales_pipeline') }}