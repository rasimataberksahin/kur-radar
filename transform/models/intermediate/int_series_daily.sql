-- Every modelled price series in one long table: (series_id, obs_date, value).
-- Each series keeps its own calendar (TCMB bulletin days, COMEX trading days).
select 'USDTRY' as series_id, rate_date as obs_date, mid_try as value
from {{ ref('fct_fx_daily') }} where currency_code = 'USD'
union all
select 'EURTRY', rate_date, mid_try
from {{ ref('fct_fx_daily') }} where currency_code = 'EUR'
union all
select 'GRAMTRY', price_date, gram_try
from {{ ref('fct_gold_gram_daily') }} where is_final
union all
select 'XAUUSD', price_date, ounce_usd
from {{ ref('stg_gold__ounce_daily') }} where is_final
union all
select 'EURUSD', rate_date, rate
from {{ ref('fct_fx_cross_daily') }} where pair = 'EURUSD'
