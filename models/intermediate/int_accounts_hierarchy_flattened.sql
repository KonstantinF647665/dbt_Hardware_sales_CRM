WITH accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),
processed AS (
    SELECT 
        *,
        CASE 
            WHEN parent_account IS NULL 
                 OR parent_account IN ('0', '') 
            THEN account_name 
            ELSE parent_account 
        END AS ultimate_parent_name
    FROM accounts
)
SELECT * FROM processed
WHERE ultimate_parent_name != '0'