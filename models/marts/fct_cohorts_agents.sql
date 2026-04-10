WITH base AS (
    SELECT 
        deal_id,
        DATE_TRUNC('month', engage_date)::DATE AS cohort_m,
        DATE_TRUNC('month', close_date)::DATE AS close_m,
        deal_stage,
        deal_value
    FROM {{ ref('int_deals_and_teams') }}
    WHERE close_date IS NOT NULL
),
cohort_sizes AS (
    SELECT 
        cohort_m,
        COUNT(deal_id) AS deals_b
    FROM base
    GROUP BY 1
),
monthly_activity AS (
    SELECT
        cohort_m,
        close_m,
        (EXTRACT(YEAR FROM close_m) - EXTRACT(YEAR FROM cohort_m)) * 12 +
        (EXTRACT(MONTH FROM close_m) - EXTRACT(MONTH FROM cohort_m)) AS cohort_duration,
        COUNT(deal_id) AS closed_this_month,
        SUM(CASE WHEN deal_stage = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_deals_this_month,
        SUM(deal_value) AS revenue_this_month
    FROM base
    GROUP BY 1, 2, 3
),
final_cohort AS (
    SELECT
        m.*,
        s.deals_b,
        SUM(m.won_deals_this_month) OVER (
            PARTITION BY m.cohort_m 
            ORDER BY m.cohort_duration
        ) as won_deals_by_the_moment,
        SUM(m.won_deals_this_month) OVER (
            PARTITION BY m.cohort_m 
            ORDER BY m.cohort_duration
        ) as won_by_the_moment
    FROM monthly_activity m
    JOIN cohort_sizes s ON m.cohort_m = s.cohort_m
)
SELECT 
    cohort_m,
    close_m,
    ROUND(won_by_the_moment::NUMERIC / NULLIF(deals_b, 0) * 100, 2) AS Конверсия,
    revenue_this_month,
    cohort_duration,
    deals_b,
    closed_this_month,
    won_by_the_moment,
    (deals_b - won_by_the_moment) AS left_open,
    won_deals_this_month
FROM final_cohort
ORDER BY cohort_m, cohort_duration