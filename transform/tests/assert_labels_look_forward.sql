-- Every label is strictly after its observation date (and only labels look forward).
select series_id, obs_date, horizon, label_date
from {{ ref('fct_labels_daily') }}
where label_date <= obs_date
