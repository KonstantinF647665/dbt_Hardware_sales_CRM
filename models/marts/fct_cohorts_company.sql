WITH base AS (
    SELECT 
        Идентификатор_сделки,
        DATE_TRUNC('month', Активная_стадия_с)::date as Когорта_мес,
        DATE_TRUNC('month', Работа_по_сделке_окончена)::date as Закрыта_мес,
        Статус_сделки,
        Цена_сделки
    FROM {{ ref('int_deals_and_teams') }}
    WHERE Работа_по_сделке_окончена IS NOT NULL
),
cohort_sizes AS (
    SELECT 
        Когорта_мес,
        COUNT(Идентификатор_сделки) as Исходно_сделок
    FROM base
    GROUP BY 1
),
monthly_activity AS (
    SELECT
        Когорта_мес,
        Закрыта_мес,
        (EXTRACT(YEAR FROM Закрыта_мес) - EXTRACT(YEAR FROM Когорта_мес)) * 12 +
        (EXTRACT(MONTH FROM Закрыта_мес) - EXTRACT(MONTH FROM Когорта_мес)) AS Возраст_мес,
        COUNT(Идентификатор_сделки) AS Закрыто_в_этом_месяце,
        SUM(CASE WHEN Статус_сделки = 'Сделка заключена' THEN 1 ELSE 0 END) AS Успешно_в_этом_месяце,
        SUM(Цена_сделки) AS Доход_в_этом_месяце
    FROM base
    GROUP BY 1, 2, 3
),
final_cohort AS (
    SELECT
        m.*,
        s.Исходно_сделок,
        SUM(m.Закрыто_в_этом_месяце) OVER (
            PARTITION BY m.Когорта_мес 
            ORDER BY m.Возраст_мес
        ) as Всего_закрыто_на_текущий_момент,
        SUM(m.Успешно_в_этом_месяце) OVER (
            PARTITION BY m.Когорта_мес 
            ORDER BY m.Возраст_мес
        ) as Всего_успешно_на_текущий_момент
    FROM monthly_activity m
    JOIN cohort_sizes s ON m.Когорта_мес = s.Когорта_мес
)
SELECT 
    Когорта_мес,
    ROUND(Всего_успешно_на_текущий_момент::numeric / NULLIF(Исходно_сделок, 0) * 100, 2) as Накопительная_конверсия_ПКТО,
    Закрыта_мес,
    Возраст_мес,
    Исходно_сделок,
    Закрыто_в_этом_месяце,
    Всего_закрыто_на_текущий_момент,
    (Исходно_сделок - Всего_закрыто_на_текущий_момент) as Осталось_открытых,
    Успешно_в_этом_месяце,
    Доход_в_этом_месяце
FROM final_cohort
ORDER BY Когорта_мес, Возраст_мес