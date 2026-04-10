WITH deals_with_offices AS (
    SELECT 
        o.Цена_сделки,
        o.Статус_сделки,
        t.Отделение,
        o.Имя_агента
    FROM {{ ref('stg_opportunities') }} o
    LEFT JOIN {{ ref('stg_sales_teams') }} t ON o.Имя_агента = t.Имя_агента
)
SELECT 
    Отделение,
    SUM(Цена_сделки) AS Сумма_сделок,
    SUM(CASE WHEN Статус_сделки = 'Сделка заключена' THEN 1 ELSE 0 END) AS Всего_заключено,
    COUNT(DISTINCT Имя_агента) AS Агентов_всего_в_отделении,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY Цена_сделки) 
        FILTER (WHERE Цена_сделки > 0) AS Медианная_сумма_сделки_в_отделении,
    ROUND(AVG(Цена_сделки) FILTER (WHERE Цена_сделки > 0), 2) AS Среднее_арифметическое_сделки
FROM deals_with_offices
GROUP BY 1
ORDER BY Сумма_сделок ASC