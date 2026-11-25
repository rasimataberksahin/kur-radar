-- The 2005 redenomination (÷1,000,000) and per-unit scaling must not create a jump:
-- last old-lira bulletin vs. first new-lira bulletin within 5% for every currency in both.
-- Only checked when the two bulletins are close together (both sides actually loaded).
with edge as (
    select
        currency_code,
        max(rate_date) filter (where rate_date < date '2005-01-01')  as last_old,
        min(rate_date) filter (where rate_date >= date '2005-01-01') as first_new
    from {{ ref('fct_fx_daily') }}
    group by currency_code
)

select e.currency_code, o.mid_try as old_mid, n.mid_try as new_mid
from edge as e
join {{ ref('fct_fx_daily') }} as o on o.currency_code = e.currency_code and o.rate_date = e.last_old
join {{ ref('fct_fx_daily') }} as n on n.currency_code = e.currency_code and n.rate_date = e.first_new
where e.first_new - e.last_old <= 7
  and abs(n.mid_try / o.mid_try - 1) > 0.05
