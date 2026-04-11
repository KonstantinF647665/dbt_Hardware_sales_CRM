WITH base_info AS (
    SELECT 
        s_o.*,
        s_p.suggested_price
    FROM {{ ref('stg_opportunities') }} s_o
    LEFT JOIN {{ ref('stg_products')}} AS s_p ON s_o.product_name = s_p.product_name
),
agent_totals AS (
    SELECT 
        agent_name,
        SUM(deal_value) AS total_deals_value_agent,
        COUNT(deal_stage) AS total_deals_count_agent,
        ROUND(AVG(deal_value) FILTER (WHERE deal_value > 0), 2) AS avg_deal_value,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY deal_value) FILTER (WHERE deal_value > 0) AS median_deal_value,
        COUNT(DISTINCT deal_id) AS deals_count_agent,
        COUNT(DISTINCT product_name) AS unique_products_count,
        SUM(CASE WHEN deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_deals_agent,
        SUM(CASE WHEN deal_stage = 'Сделка не состоялась' THEN 1 ELSE 0 END) AS lost_deals_agent,
        SUM(CASE WHEN deal_stage IN ('Работа с клиентом', 'Разведка') THEN 1 ELSE 0 END) AS engaged_deals_count,
        ROUND(SUM(close_date - engage_date) / NULLIF(COUNT(DISTINCT deal_id), 0)) AS avg_days_per_deal
    FROM base_info
    GROUP BY 1
),
agent_success_stats AS (
    SELECT 
        agent_name,
        SUM(deal_value - suggested_price) AS margin,
        ROUND(AVG(deal_value - suggested_price), 3) AS typical_diverson,
        ROUND(AVG((deal_value - suggested_price) / NULLIF(suggested_price, 0)) * 100, 2) AS percent_of_mistakes
    FROM base_info
    WHERE deal_stage = 'Сделка заключена'
    GROUP BY 1
)
SELECT 
    t.agent_name,
    t.total_deals_value_agent,
    t.total_deals_count_agent,
    s.margin,
    s.typical_diverson AS typical_diverson_from_the_price_by_one_deal,
    s.percent_of_mistakes,
    t.avg_deal_value,
    t.median_deal_value,
    t.unique_products_count,
    t.won_deals_agent,
    t.lost_deals_agent,
    t.engaged_deals_count,
    t.avg_days_per_deal
FROM agent_totals t
LEFT JOIN agent_success_stats s ON t.agent_name = s.agent_name
ORDER BY t.total_deals_value_agent DESC