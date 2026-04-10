WITH accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),
processed AS (
    SELECT 
        *,
        CASE 
            WHEN Головное_предприятие IS NULL 
                 OR Головное_предприятие IN ('0', '') 
            THEN Название_компании 
            ELSE Головное_предприятие 
        END AS Итоговое_название_группы
    FROM accounts
)
SELECT * FROM processed
WHERE Итоговое_название_группы != '0'