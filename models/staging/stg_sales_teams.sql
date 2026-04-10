select LOWER(TRIM(sales_agent::text)) agent_name,
LOWER(TRIM(manager::text)) agent_manager,
CASE
WHEN LOWER(TRIM(regional_office::text)) LIKE 'central' THEN 'Головное отделение'
WHEN LOWER(TRIM(regional_office::text)) LIKE 'east' THEN 'Восточное отделение'
WHEN LOWER(TRIM(regional_office::text)) LIKE 'west' THEN 'Западное отделение'
ELSE 'Другое'
END regional_office
from {{ ref('sales_teams') }}