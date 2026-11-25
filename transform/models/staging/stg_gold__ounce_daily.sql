-- Daily ounce gold in USD per troy ounce (COMEX futures settlement by default).
select
    provider,
    symbol,
    price_date,
    close            as ounce_usd,
    open             as open_usd,
    high             as high_usd,
    low              as low_usd,
    volume,
    ohlc_consistent,
    is_final,
    source_sha256
from {{ source('raw', 'gold_ounce_daily') }}
where provider = 'yahoo' and symbol = 'GC=F'
