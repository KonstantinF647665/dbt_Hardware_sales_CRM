WITH accs_aggregated AS (
    SELECT 
        industry,
        SUM(annual_revenue) AS total_revenue,
        SUM(employee_count) AS total_employees,
        COUNT(account_name) AS count_companies
    FROM {{ref("int_accounts_hierarchy_flattened")}}
    GROUP BY industry
),
opps_aggregated AS (
    SELECT 
        a.industry,
        COUNT(DISTINCT opp.agent_name) AS count_agents,
        COUNT(DISTINCT opp.deal_id) AS count_opps,
        COUNT(DISTINCT opp.product_name) AS count_unique_products,
        SUM(CASE WHEN opp.deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_opps,
        SUM(CASE WHEN opp.deal_stage = 'Сделка не состоялась' THEN 1 ELSE 0 END) AS lost_opps,
        SUM(CASE WHEN opp.deal_stage IN ('Работа с клиентом', 'Разведка') THEN 1 ELSE 0 END) AS engaged_deals_count,
        COUNT(deal_stage) AS overall_deals_count,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY opp.deal_value) FILTER (WHERE opp.deal_value > 0) AS median_deal_value,
        ROUND(
            AVG(opp.deal_value), 
            2) AS avg_deal_value_industry,
        ROUND(
            SUM(CASE WHEN opp.deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END)* 1.0 / NULLIF(COUNT(deal_stage), 0),
            2) won_deals_conversion_industry,
        SUM(opp.deal_value) AS total_opp_value,
        AVG(
            CASE 
                WHEN opp.close_date > opp.engage_date 
                THEN (opp.close_date - opp.engage_date)
                ELSE NULL 
            END
        ) AS avg_days_per_opp
    FROM {{ref('int_deals_and_teams')}} opp
    LEFT JOIN accs_aggregated a ON opp.industry = a.industry
    GROUP BY a.industry
)    
SELECT 
    acc.industry,
    ROUND
    (
        ((opp.total_opp_value * 100) / NULLIF(SUM(opp.total_opp_value) OVER (), 0))
    ,2) AS part_in_total_deals_value,
    ROUND(
        ((opp.won_opps * 100) / NULLIF(SUM(opp.won_opps) OVER (), 0))
    ,2) AS percent_of_won_deals_from_all_won_deals,
    avg_deal_value_industry,
    median_deal_value,
    won_deals_conversion_industry,
    COALESCE(opp.total_opp_value, 0) AS total_deals_value,
    COALESCE(opp.won_opps, 0) AS won_deals_industry,
    COALESCE(opp.lost_opps, 0) AS lost_deals_industry,
    COALESCE(opp.count_agents, 0) AS agents_indutry,
    COALESCE(acc.total_revenue, 0) AS annual_revenue,
    acc.total_employees AS total_employees,
    COALESCE(opp.count_unique_products, 0) AS count_unique_products,
    ROUND(COALESCE(opp.avg_days_per_opp, 0)) AS avg_days_per_deal,
    acc.count_companies AS accounts_in_the_industry
FROM accs_aggregated acc
LEFT JOIN opps_aggregated opp ON acc.industry = opp.industry
ORDER BY part_in_total_deals_value DESC