-- Forward-looking targets, kept apart from the features on purpose: one row per
-- (series, obs_date, horizon) with the log return to the observation `horizon` steps ahead.
-- Rows whose future observation doesn't exist yet are absent.
with s as (
    select series_id, obs_date, value
    from {{ ref('int_series_daily') }}
    where series_id in ({{ "'" ~ var('target_series') | join("', '") ~ "'" }}) and value > 0
),

h as (
    select unnest(array[{{ var('horizons') | join(', ') }}]) as horizon
),

ahead as (
    select
        s.series_id,
        s.obs_date,
        h.horizon,
        s.value,
        lead(s.obs_date, h.horizon) over w as label_date,
        lead(s.value, h.horizon) over w    as future_value
    from s
    cross join h
    window w as (partition by s.series_id, h.horizon order by s.obs_date)
)

select
    series_id,
    obs_date,
    horizon,
    label_date,
    value,
    future_value,
    ln(future_value / value)   as fwd_log_return,
    future_value > value       as is_up
from ahead
where future_value is not null
