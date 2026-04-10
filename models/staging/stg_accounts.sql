select COALESCE(NULLIF(TRIM(LOWER(account)), ''), '0') Название_компании,
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
End Отрасль,
(year_established || '-01-01')::date Дата_основания,
revenue Годовой_доход_млн_долларов,
employees Количество_работников,
TRIM(LOWER(office_location)) Адрес_компании,
COALESCE(NULLIF(LOWER(TRIM(subsidiary_of)), ''), '0') Головное_предприятие
from {{ ref('accounts') }}