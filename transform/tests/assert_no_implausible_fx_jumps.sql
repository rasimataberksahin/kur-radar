-- Major currencies never move more than 40% (log) between consecutive bulletins.
-- The largest real move is the February 2001 float (USD ≈ +32%); anything bigger is a
-- unit or redenomination error. Only consecutive bulletins (≤ 10 days apart) are compared,
-- so a gap in the data isn't mistaken for a jump.
select rate_date, currency_code, log_return_1d
from {{ ref('fct_fx_daily') }}
where currency_code in ('USD', 'EUR', 'GBP', 'CHF', 'JPY')
  and days_since_prev <= 10
  and abs(log_return_1d) > 0.40
