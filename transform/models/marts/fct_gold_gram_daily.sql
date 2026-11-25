{{ config(indexes=[{'columns': ['price_date']}]) }}
-- Historical gram gold in TRY: ounce close (USD) × TCMB USD/TRY ÷ grams per troy ounce.
-- USD/TRY is taken as of the ounce date (the latest bulletin on or before it), so Turkish
-- holidays reuse the previous bulletin; usd_rate_age_days shows how stale it is. If the
-- latest bulletin is older than max_usd_rate_age_days (a gap in the data), the day is left
-- out rather than priced with a stale rate.
with ounce as (
    select * from {{ ref('stg_gold__ounce_daily') }}
),

usd as (
    select rate_date, mid_try from {{ ref('fct_fx_daily') }} where currency_code = 'USD'
)

select
    o.price_date,
    o.ounce_usd,
    o.is_final,
    o.ohlc_consistent,
    u.rate_date                                                  as usd_rate_date,
    o.price_date - u.rate_date                                   as usd_rate_age_days,
    u.mid_try                                                    as usd_try,
    o.ounce_usd / {{ var('grams_per_troy_ounce') }}              as gram_usd,
    o.ounce_usd * u.mid_try / {{ var('grams_per_troy_ounce') }}  as gram_try
from ounce as o
cross join lateral (
    select rate_date, mid_try from usd
    where usd.rate_date <= o.price_date
    order by usd.rate_date desc
    limit 1
) as u
where o.price_date - u.rate_date <= {{ var('max_usd_rate_age_days') }}
