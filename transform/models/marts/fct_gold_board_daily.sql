-- Turkish gold products at the day's last board snapshot (Istanbul date), with two premiums:
--   premium_over_has_pct:  price vs. its fine-gold content valued at the board's own HAS price
--   premium_over_intl_pct: price vs. its fine-gold content valued at the international
--                          price (fct_gold_gram_daily, latest ounce on or before the day)
with quotes as (
    select * from {{ ref('stg_truncgil__quotes') }}
),

last_snapshot as (
    select observed_date, max(observed_at) as observed_at
    from quotes
    where code = 'GRA'
    group by observed_date
),

board as (
    select q.*, p.product_tr, p.product_en, p.gross_grams, p.fineness, p.display_order
    from last_snapshot as l
    join quotes as q on q.observed_at = l.observed_at
    join {{ ref('gold_products') }} as p on p.code = q.code
),

has as (
    select observed_at, mid / fineness as fine_gram_try  -- HAS is 995: price per pure gram
    from board where code = 'HAS'
)

select
    b.observed_date,
    b.observed_at,
    b.code,
    b.product_tr,
    b.product_en,
    b.display_order,
    b.buying,
    b.selling,
    b.mid,
    b.bid_ask_spread_pct,
    b.gross_grams * b.fineness                                          as fine_grams,
    b.mid / nullif(h.fine_gram_try * b.gross_grams * b.fineness, 0) - 1 as premium_over_has_pct,
    g.price_date                                                        as intl_price_date,
    g.gram_try                                                          as intl_fine_gram_try,
    b.mid / nullif(g.gram_try * b.gross_grams * b.fineness, 0) - 1      as premium_over_intl_pct
from board as b
left join has as h on h.observed_at = b.observed_at
left join lateral (
    select price_date, gram_try from {{ ref('fct_gold_gram_daily') }} as gg
    where gg.price_date <= b.observed_date
    order by gg.price_date desc
    limit 1
) as g on true
