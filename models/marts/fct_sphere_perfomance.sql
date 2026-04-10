WITH accs_aggregated AS (
    SELECT 
        Отрасль,
        SUM(Годовой_доход_млн_долларов) AS total_revenue,
        SUM(Количество_работников) AS total_employees,
        COUNT(Название_компании) AS count_companies
    FROM {{ref("int_accounts_hierarchy_flattened")}}
    GROUP BY Отрасль
),
opps_aggregated AS (
    SELECT 
        a.Отрасль,
        COUNT(DISTINCT opp.Имя_агента) AS count_agents,
        COUNT(DISTINCT opp.Идентификатор_сделки) AS count_opps,
        COUNT(DISTINCT opp.Название_продукта) AS count_unique_products,
        SUM(CASE WHEN opp.Статус_сделки = 'Сделка заключена' THEN 1 ELSE 0 END) AS won_opps,
        SUM(CASE WHEN opp.Статус_сделки = 'Сделка не состоялась' THEN 1 ELSE 0 END) AS lost_opps,
        SUM(CASE WHEN opp.Статус_сделки IN ('Работа с клиентом', 'Разведка') THEN 1 ELSE 0 END) AS Сделок_в_работе,
        COUNT(Статус_сделки) AS Всего_сделок,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY opp.Цена_сделки) FILTER (WHERE opp.Цена_сделки > 0) AS Медианная_цена_сделки_отрасль,
        ROUND(
            AVG(opp.Цена_сделки), 
            2) AS Средняя_цена_сделки_отрасль,
        ROUND(
            SUM(CASE WHEN opp.Статус_сделки = 'Сделка заключена' THEN 1 ELSE 0 END)* 1.0 / NULLIF(COUNT(Статус_сделки), 0),
            2) Конверсия_в_заключенные_сделки_сфера,
        SUM(opp.Цена_сделки) AS total_opp_value,
        AVG(
            CASE 
                WHEN opp.Работа_по_сделке_окончена > opp.Активная_стадия_с 
                THEN (opp.Работа_по_сделке_окончена - opp.Активная_стадия_с)
                ELSE NULL 
            END
        ) AS avg_days_per_opp
    FROM {{ref('int_deals_and_teams')}} opp
    LEFT JOIN accs_aggregated a ON opp.Сфера_деятельности_компании = a.Отрасль
    GROUP BY a.Отрасль
)    
SELECT 
    acc.Отрасль,
    ROUND
    (
        ((opp.total_opp_value * 100) / NULLIF(SUM(opp.total_opp_value) OVER (), 0))
    ,2) AS Доля_сделок_от_общей_суммы_сделок,
    ROUND(
        ((opp.won_opps * 100) / NULLIF(SUM(opp.won_opps) OVER (), 0))
    ,2) AS Процент_сделок_отрасли_от_общего_числа_сделок,
    Средняя_цена_сделки_отрасль,
    Медианная_цена_сделки_отрасль,
    Конверсия_в_заключенные_сделки_сфера,
    COALESCE(opp.total_opp_value, 0) AS Суммарно_сделок_на,
    COALESCE(opp.won_opps, 0) AS Заключено_сделок_в_отрасли,
    COALESCE(opp.lost_opps, 0) AS Неудачных_сделок_в_отрасли,
    COALESCE(opp.count_agents, 0) AS Агентов_работает_по_отрасли,
    COALESCE(acc.total_revenue, 0) AS Годовой_доход_млн_долларов,
    acc.total_employees AS Количество_работников,
    COALESCE(opp.count_unique_products, 0) AS Уникальных_товарных_категорий,
    ROUND(COALESCE(opp.avg_days_per_opp, 0)) AS Среднее_дней_на_сделку,
    acc.count_companies AS Кол_во_компаний_в_отрасли
FROM accs_aggregated acc
LEFT JOIN opps_aggregated opp ON acc.Отрасль = opp.Отрасль
ORDER BY Доля_сделок_от_общей_суммы_сделок DESC