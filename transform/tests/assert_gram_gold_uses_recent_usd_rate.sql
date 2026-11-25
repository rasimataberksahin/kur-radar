-- The TCMB USD rate used for an ounce date is never more than 10 days old
-- (longest Turkish bulletin gap, e.g. a 9-day bayram holiday).
select price_date, usd_rate_date, usd_rate_age_days
from {{ ref('fct_gold_gram_daily') }}
where usd_rate_age_days > {{ var('max_usd_rate_age_days') }}
