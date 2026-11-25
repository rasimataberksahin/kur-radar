-- Every Truncgil board quote with its Istanbul date and a mid price.
select
    observed_at,
    (observed_at at time zone 'Europe/Istanbul')::date   as observed_date,
    code,
    instrument,
    name,
    buying,
    selling,
    coalesce((buying + selling) / 2, buying, selling)     as mid,
    case when buying > 0 and selling > 0
         then (selling - buying) / ((buying + selling) / 2) end as bid_ask_spread_pct,
    change_pct,
    source_sha256
from {{ source('raw', 'truncgil_quotes') }}
