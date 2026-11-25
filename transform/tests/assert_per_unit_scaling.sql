-- JPY must be per ONE yen: always between 1/1000 and 1/50 of a dollar's TRY price.
-- Catches a missing ÷unit (JPY is quoted per 100 since 2005).
select j.rate_date, j.mid_try as jpy, u.mid_try as usd
from {{ ref('fct_fx_daily') }} as j
join {{ ref('fct_fx_daily') }} as u on u.rate_date = j.rate_date and u.currency_code = 'USD'
where j.currency_code = 'JPY'
  and not (j.mid_try between u.mid_try / 1000 and u.mid_try / 50)
