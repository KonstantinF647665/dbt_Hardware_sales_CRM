WITH opportunities AS (
    SELECT 
        *,
        (close_date - engage_date) AS days_to_close
    FROM {{ ref('stg_opportunities') }}
),
hierarchy AS (
    SELECT * FROM {{ ref('int_accounts_hierarchy_flattened') }}
),
grouped_metrics AS (
    SELECT 
        h.ultimate_parent_name,
        MIN(h.year_established) AS year_established,
        SUM(h.annual_revenue) AS annual_revenue_companies,
        SUM(h.employee_count) AS employee_count_companies,
        MAX(h.office_location) AS office_location,
--        COUNT(DISTINCT CASE 
--            WHEN h.parent_account != h.account_name THEN h.account_name 
--        END) AS number_of_subsidiaries,
        COUNT(DISTINCT o.agent_name) AS agents_count,
        COUNT(DISTINCT o.deal_id) AS deal_count,
        COUNT(DISTINCT o.product_name) AS number_of_unique_products,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY o.deal_value) FILTER (WHERE o.deal_value > 0) AS median_deal_value_group,
        ROUND(AVG(o.deal_value), 2) AS avg_deal_value_companies,
        SUM(CASE WHEN o.deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_deals,
        SUM(CASE WHEN o.deal_stage = 'Сделка не состоялась' THEN 1 ELSE 0 END) AS lost_deals,
        SUM(CASE WHEN deal_stage IN ('Работа с клиентом', 'Разведка') THEN 1 ELSE 0 END) AS engage_state_deals,
        COUNT(deal_stage) AS all_deals,
        SUM(o.deal_value) AS all_deals_value,
        SUM(o.days_to_close) AS days_to_close,
        ROUND(
            SUM(CASE WHEN o.deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END)* 1.0 / NULLIF(COUNT(deal_stage), 0)
            ,2) conversion_in_a_group
    FROM hierarchy h
    LEFT JOIN opportunities o ON o.account_name = h.ultimate_parent_name
    GROUP BY h.ultimate_parent_name
)
SELECT 
    ultimate_parent_name AS ultimate_group_name,
    ROUND(
    ((all_deals_value / (SELECT COALESCE(SUM(all_deals_value),0) FROM grouped_metrics)) *100)
    ,2) AS part_in_all_deals_value,
    ROUND(
    (won_deals / (SELECT COALESCE(SUM(won_deals),0) FROM grouped_metrics) *100)
    ,2) AS part_in_won_deals,
    COALESCE(avg_deal_value_companies, 0) AS avg_deal_value_companies,
    COALESCE(median_deal_value_group, 0) AS median_deal_value_group,
    conversion_in_a_group conversion_in_a_group,
    COALESCE(all_deals_value, 0) AS all_deals_value,
    COALESCE(won_deals, 0) AS won_deals_group,
    COALESCE(lost_deals, 0) AS lost_deals_group,
    COALESCE(engage_state_deals, 0) AS engage_state_deals_group,
    COALESCE(employee_count_companies, 0) AS employee_count_group,
    annual_revenue_companies AS annual_revenue_companies,
    COALESCE(number_of_unique_products, 0) AS number_of_bought_unique_products,
    ROUND(days_to_close / NULLIF(all_deals, 0)) AS avg_days_to_close_deal,
    year_established,
    office_location AS office_location
FROM grouped_metrics
ORDER BY part_in_all_deals_value DESC