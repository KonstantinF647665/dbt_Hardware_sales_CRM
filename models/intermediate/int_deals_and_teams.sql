WITH sales AS (
    SELECT * FROM {{ref("stg_opportunities")}}
),
sales_teams AS (
    SELECT agent_name, agent_manager FROM {{ref("stg_sales_teams")}}
),
suggested_price AS (
    SELECT product_name, suggested_price FROM {{ref("stg_products")}}
),
accounts AS (
    SELECT account_name, industry
    FROM {{ref("stg_accounts")}}
)
SELECT sales.deal_id,
sales.agent_name,
sales.product_name,
sales.account_name,
sales.deal_stage,
sales.engage_date,
sales.close_date,
sales.deal_value,
suggested_price.suggested_price,
sales_teams.agent_manager,
accounts.industry
FROM sales
LEFT JOIN sales_teams ON sales.agent_name = sales_teams.agent_name
LEFT JOIN suggested_price ON sales.product_name = suggested_price.product_name
LEFT JOIN accounts ON sales.account_name = accounts.account_name