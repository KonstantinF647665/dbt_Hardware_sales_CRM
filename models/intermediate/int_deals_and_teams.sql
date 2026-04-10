WITH sales AS (
    SELECT * FROM {{ref("stg_opportunities")}}
),
sales_teams AS (
    SELECT Имя_агента, Руководитель_агента FROM {{ref("stg_sales_teams")}}
),
suggested_price AS (
    SELECT Название_продукта, Прогнозная_стоимость_товара FROM {{ref("stg_products")}}
),
accounts AS (
    SELECT Название_компании, Отрасль
    FROM {{ref("stg_accounts")}}
)
SELECT sales.Идентификатор_сделки,
sales.Имя_агента,
sales.Название_продукта,
sales.Название_компании,
sales.Статус_сделки,
sales.Активная_стадия_с,
sales.Работа_по_сделке_окончена,
sales.Цена_сделки,
suggested_price.Прогнозная_стоимость_товара,
sales_teams.Руководитель_агента,
accounts.Отрасль Сфера_деятельности_компании
FROM sales
LEFT JOIN sales_teams ON sales.Имя_агента = sales_teams.Имя_агента
LEFT JOIN suggested_price ON sales.Название_продукта = suggested_price.Название_продукта
LEFT JOIN accounts ON sales.Название_компании = accounts.Название_компании