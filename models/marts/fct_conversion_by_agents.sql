SELECT 
agent_name,
ROUND(
    ((total_deals_value_agent / (SELECT COALESCE(SUM(total_deals_value_agent),0) FROM {{ref('int_agents_aggr')}})) *100),
     2) AS value_part_in_total_deals_value,
ROUND(
    ((won_deals_agent / (SELECT COALESCE(SUM(won_deals_agent),0) FROM {{ref('int_agents_aggr')}})) *100),
     2) AS successful_deals_part_in_total_successful_deals,
avg_deal_value,
median_deal_value,
ROUND(
    (won_deals_agent * 1.0 / NULLIF(total_deals_count_agent, 0)) ,
     2) AS conversion_by_agent
FROM {{ref('int_agents_aggr')}}
ORDER BY value_part_in_total_deals_value DESC