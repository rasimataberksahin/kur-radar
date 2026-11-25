{{ config(indexes=[{'columns': ['currency_code', 'rate_date']}]) }}
-- One row per bulletin day and currency, in new lira per one unit (see stg_tcmb__rates).
with rates as (
    select * from {{ ref('stg_tcmb__rates') }}
    where mid_try is not null
)

select
    rate_date,
    currency_code,
    name_en,
    quoted_unit,
    is_old_lira,
    forex_buying_try,
    forex_selling_try,
    banknote_buying_try,
    banknote_selling_try,
    mid_try,
    {{ log_return('mid_try', 'currency_code', 'rate_date', 1) }} as log_return_1d,
    rate_date - lag(rate_date) over (partition by currency_code order by rate_date)
        as days_since_prev
from rates
