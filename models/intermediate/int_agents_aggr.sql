WITH opp_selected_info AS (
    SELECT 
        s_o.agent_name,
        s_o.deal_id,
        s_o.product_name,
        s_o.deal_stage,
        s_o.deal_value,
        s_p.suggested_price,
        (close_date - engage_date) AS days_to_close
    FROM {{ ref('stg_opportunities') }} s_o
    LEFT JOIN {{ ref('stg_products')}} AS s_p ON s_o.product_name = s_p.product_name
),
succ_deals AS (
    SELECT agent_name,
    deal_value,
    suggested_price,
    (deal_price - suggested_price) diff
    FROM opp_selected_info
    WHERE deal_stage = 'Сделка заключена'
)
SELECT 
    o_s_i.agent_name,
    SUM(o_s_i.deal_value) AS total_deals_value,
    COUNT(o_s_i.deal_stage) AS total_successful_deals_count,
    ROUND(AVG(o_s_i.deal_value) FILTER (WHERE o_s_i.deal_value > 0), 2)  AS avg_deal_value,
    PERCENTILE_DISC (0.5) WITHIN GROUP (ORDER BY o_s_i.deal_value) FILTER (WHERE o_s_i.deal_value > 0) AS median_deal_value,
    SUM(s_d.diff) AS margin,
    ROUND(AVG(s_d.diff), 3) AS Typical_diverson_from_the_price_by_one_deal,
    ROUND(AVG((s_d.deal_value - s_d.suggested_price) / NULLIF(s_d.suggested_price, 0)) * 100, 2) Percent_of_mistakes,
    COUNT(DISTINCT o_s_i.deal_id) AS deals_count_agent,
    COUNT(DISTINCT o_s_i.product_name) AS unique_products_count,
    SUM(CASE WHEN o_s_i.deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_deals_agent,
    SUM(CASE WHEN o_s_i.deal_stage = 'Сделка не состоялась' THEN 1 ELSE 0 END) AS lost_deals_agent,
    SUM(CASE WHEN o_s_i.deal_stage IN ('Работа с клиентом', 'Разведка') THEN 1 ELSE 0 END) AS overall_deals_count,
    ROUND(SUM(o_s_i.days_to_close) / NULLIF(COUNT(DISTINCT o_s_i.deal_id), 0)) AS avg_days_per_deal
FROM opp_selected_info o_s_i
LEFT JOIN succ_deals s_d ON o_s_i.agent_name = s_d.agent_name
GROUP BY 1
ORDER BY total_deals_value DESC