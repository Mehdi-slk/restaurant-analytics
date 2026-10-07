{% snapshot snap_restaurant %}
{{ config(target_schema='snapshots', unique_key='restaurant_business_id',
          strategy='check', check_cols=['restaurant_name','cuisine']) }}
select distinct on (restaurant_business_id)
    restaurant_business_id,
    coalesce(restaurant_name,'Unknown') as restaurant_name,
    coalesce(cuisine,'Unknown')         as cuisine
from {{ ref('int_restaurant_inspections_recovered') }}
order by restaurant_business_id,
         record_datetime desc nulls last, inspection_datetime desc nulls last
{% endsnapshot %}