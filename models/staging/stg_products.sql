select LOWER(REPLACE(product::text, ' ', '')) product_name,
LOWER(TRIM(series::text)) product_series,
COALESCE(sales_price::numeric, 0) suggested_price
from {{ source('raw_crm', 'products') }}