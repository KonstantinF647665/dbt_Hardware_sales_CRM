select COALESCE(NULLIF(TRIM(LOWER(account)), ''), '0') account_name,
CASE 
    WHEN TRIM(sector) LIKE 'technology' THEN 'Технологии'
    WHEN TRIM(sector) LIKE 'technolgy' THEN 'Технологии'
    WHEN TRIM(sector) LIKE 'medical' THEN 'Медицина'
    WHEN TRIM(sector) LIKE 'retail' THEN 'Торговля'
    WHEN TRIM(sector) LIKE 'entertainment' THEN 'Развлечения'
    WHEN TRIM(sector) LIKE 'software' THEN 'Программное обеспечение'
    WHEN TRIM(sector) LIKE 'telecommunications' THEN 'Телекоммуникация'
    WHEN TRIM(sector) LIKE 'finance' THEN 'Финансы'
    WHEN TRIM(sector) LIKE 'services' THEN 'Сфера услуг'
    WHEN TRIM(sector) LIKE 'marketing' THEN 'Маркетинг'
    WHEN TRIM(sector) LIKE 'employment' THEN 'Рекрутинг'
    ELSE 'Другое'
End industry,
(year_established || '-01-01')::date,
revenue annual_revenue,
employees employee_count,
TRIM(LOWER(office_location)),
COALESCE(NULLIF(LOWER(TRIM(subsidiary_of)), ''), '0') parent_account
from {{ ref('accounts') }}