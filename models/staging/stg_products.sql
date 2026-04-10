select LOWER(REPLACE(product::text, ' ', '')) Название_продукта,
LOWER(TRIM(series::text)) Модель,
COALESCE(sales_price::numeric, 0) Прогнозная_стоимость_товара
from {{ ref('products') }}