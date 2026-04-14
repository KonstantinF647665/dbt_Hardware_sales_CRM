SELECT opportunity_id,
close_date,
engage_date
FROM {{ source("raw_crm", "sales_pipeline") }}
WHERE close_date < engage_date