WITH deals_with_offices AS (
    SELECT 
        o.deal_value,
        o.deal_stage,
        t.regional_office,
        o.agent_name
    FROM {{ ref('stg_opportunities') }} o
    LEFT JOIN {{ ref('stg_sales_teams') }} t ON o.agent_name = t.agent_name
)
SELECT 
    regional_office,
    SUM(deal_value) AS total_deal_value,
    SUM(CASE WHEN deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_deals_overall,
    COUNT(DISTINCT agent_name) AS agents_at_regional_office,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY deal_value)
        FILTER (WHERE deal_value > 0) AS median_deal_value_regional_office,
    ROUND(AVG(deal_value) FILTER (WHERE deal_value > 0), 2) AS deal_mean
FROM deals_with_offices
GROUP BY 1
ORDER BY total_deal_value ASC