-- The reconstructed historical gram gold must agree with the live board's gram price
-- within 10% (futures premium, timing and dealer margin explain a few percent).
select observed_date, mid, intl_fine_gram_try, premium_over_intl_pct
from {{ ref('fct_gold_board_daily') }}
where code = 'GRA'
  and intl_price_date >= observed_date - 7
  and abs(premium_over_intl_pct) > 0.10
