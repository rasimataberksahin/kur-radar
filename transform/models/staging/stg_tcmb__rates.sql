-- TCMB rates normalised to "new Turkish lira per ONE unit of the currency".
--   * redenomination: bulletins before 2005-01-01 are in old lira (1 TRY = 1,000,000 TRL)
--   * quoting unit:   e.g. JPY is quoted per 100 since 2005 (per 1 before) → divide by unit
with src as (
    select * from {{ source('raw', 'tcmb_rates') }}
),

scaled as (
    select
        *,
        (case when rate_date < date '2005-01-01' then 1000000 else 1 end * unit)::numeric
            as divisor
    from src
)

select
    rate_date,
    currency_code,
    name_en,
    name_tr,
    unit                                   as quoted_unit,
    rate_date < date '2005-01-01'          as is_old_lira,
    forex_buying / divisor                 as forex_buying_try,
    forex_selling / divisor                as forex_selling_try,
    banknote_buying / divisor              as banknote_buying_try,
    banknote_selling / divisor             as banknote_selling_try,
    -- indicative mid: forex mid, else one forex side, else banknote mid
    coalesce(
        (forex_buying + forex_selling) / 2,
        forex_buying,
        forex_selling,
        (banknote_buying + banknote_selling) / 2
    ) / divisor                            as mid_try,
    source_sha256
from scaled
