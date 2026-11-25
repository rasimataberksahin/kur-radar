{{ config(materialized="table", indexes=[{"columns": ["series_id", "obs_date"]}]) }}
-- Backward-looking features per series. Every window ends at the current row, so a feature
-- at obs_date uses only that day and earlier observations of the same series.
with base as (
    select
        series_id,
        obs_date,
        value,
        ln(value) as lv,
        row_number() over w as n_obs,
        obs_date - lag(obs_date) over w as gap_days
    from {{ ref('int_series_daily') }}
    where value > 0
    window w as (partition by series_id order by obs_date)
),

returns as (
    select
        *,
        lv - lag(lv, 1) over w  as ret_1d,
        lv - lag(lv, 5) over w  as ret_5d,
        lv - lag(lv, 20) over w as ret_20d
    from base
    window w as (partition by series_id order by obs_date)
),

windows as (
    select
        *,
        stddev_samp(ret_1d) over w20 as vol_20_raw,
        stddev_samp(ret_1d) over w60 as vol_60_raw,
        avg(value) over w20           as ma_20,
        avg(value) over w60           as ma_60,
        min(value) over w60           as min_60,
        max(value) over w60           as max_60
    from returns
    window
        w20 as (partition by series_id order by obs_date rows between 19 preceding and current row),
        w60 as (partition by series_id order by obs_date rows between 59 preceding and current row)
)

select
    series_id,
    obs_date,
    value,
    n_obs,
    gap_days,
    ret_1d,
    ret_5d,
    ret_20d,
    -- only once the window is full (20 / 60 returns)
    case when n_obs > 20 then vol_20_raw end                       as vol_20,
    case when n_obs > 60 then vol_60_raw end                       as vol_60,
    case when n_obs > 60 then vol_20_raw / nullif(vol_60_raw, 0) end as vol_ratio_20_60,
    case when n_obs >= 20 then value / ma_20 - 1 end               as ma_gap_20,
    case when n_obs >= 60 then value / ma_60 - 1 end               as ma_gap_60,
    case when n_obs >= 60 then (value - min_60) / nullif(max_60 - min_60, 0) end as range_pos_60
from windows
