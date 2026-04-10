select LOWER(TRIM(sales_agent::text)) Имя_агента,
LOWER(TRIM(manager::text)) Руководитель_агента,
CASE
WHEN LOWER(TRIM(regional_office::text)) LIKE 'central' THEN 'Головное отделение'
WHEN LOWER(TRIM(regional_office::text)) LIKE 'east' THEN 'Восточное отделение'
WHEN LOWER(TRIM(regional_office::text)) LIKE 'west' THEN 'Западное отделение'
ELSE 'Другое'
END Отделение
from {{ ref('sales_teams') }}