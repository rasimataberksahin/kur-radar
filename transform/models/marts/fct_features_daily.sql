-- Model-ready feature rows for each target series and observation date.
-- Point-in-time rule: a row for obs_date uses only data available by the end of that
-- Istanbul day. Other series are joined as of obs_date (their latest observation on or
-- before it), never later. Labels live in fct_labels_daily and must only be joined to
-- these rows by (series_id, obs_date).
{% set exogenous = ["USDTRY", "XAUUSD", "EURUSD"] %}
select
    t.series_id,
    t.obs_date,
    t.value,
    t.n_obs,
    t.gap_days,
    t.ret_1d,
    t.ret_5d,
    t.ret_20d,
    t.vol_20,
    t.vol_60,
    t.vol_ratio_20_60,
    t.ma_gap_20,
    t.ma_gap_60,
    t.range_pos_60,
    extract(isodow from t.obs_date)::int as day_of_week,
    extract(month from t.obs_date)::int  as month
    {%- for s in exogenous %}
    {%- set p = s | lower %},
    {{ p }}.obs_date as {{ p }}_asof_date,
    {{ p }}.ret_1d   as {{ p }}_ret_1d,
    {{ p }}.ret_5d   as {{ p }}_ret_5d,
    {{ p }}.vol_20   as {{ p }}_vol_20
    {%- endfor %}
from {{ ref('int_series_features') }} as t
{%- for s in exogenous %}
{%- set p = s | lower %}
left join lateral (
    -- read the table itself (indexed), not a CTE: a CTE used several times is
    -- materialised without indexes and turns every lookup into a full scan
    select obs_date, ret_1d, ret_5d, vol_20 from {{ ref('int_series_features') }} as x
    where x.series_id = '{{ s }}' and x.obs_date <= t.obs_date
    order by x.obs_date desc
    limit 1
) as {{ p }} on true
{%- endfor %}
where t.series_id in ({{ "'" ~ var('target_series') | join("', '") ~ "'" }})
