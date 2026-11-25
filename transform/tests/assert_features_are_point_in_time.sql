-- No feature row may reference an observation after its own obs_date.
select series_id, obs_date
from {{ ref('fct_features_daily') }}
where usdtry_asof_date > obs_date
   or xauusd_asof_date > obs_date
   or eurusd_asof_date > obs_date
