-- Cross rates implied by TCMB's TRY rates on the same bulletin (e.g. EURUSD = EUR/TRY ÷ USD/TRY).
select
    b.rate_date,
    p.pair,
    p.base_code,
    p.quote_code,
    b.mid_try / q.mid_try as rate
from {{ ref('fx_pairs') }} as p
join {{ ref('fct_fx_daily') }} as b on b.currency_code = p.base_code
join {{ ref('fct_fx_daily') }} as q
    on q.currency_code = p.quote_code and q.rate_date = b.rate_date
