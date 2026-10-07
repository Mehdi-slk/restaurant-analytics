with latest_phone as (
    select distinct on (restaurant_business_id)
        restaurant_business_id,
        coalesce(phone_number,'Unknown') as phone_number
    from {{ ref('int_restaurant_inspections_recovered') }}
    order by restaurant_business_id,
             record_datetime desc nulls last, inspection_datetime desc nulls last
),
versions as (
    select restaurant_business_id, restaurant_name, cuisine,
           dbt_valid_from, dbt_valid_to,
           min(dbt_valid_from) over (partition by restaurant_business_id) as first_valid_from
    from {{ ref('snap_restaurant') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['v.restaurant_business_id','v.dbt_valid_from']) }} as restaurant_key,
    v.restaurant_business_id,
    v.restaurant_name,
    v.cuisine,
    coalesce(p.phone_number,'Unknown') as phone_number,
    case when v.dbt_valid_from = v.first_valid_from
         then date '1900-01-01' else v.dbt_valid_from::date end as effective_date,
    coalesce(v.dbt_valid_to::date, date '9999-12-31')           as end_date,
    v.dbt_valid_to is null                                      as is_current
from versions v
left join latest_phone p on p.restaurant_business_id = v.restaurant_business_id

union all
select '-1','-1','Unknown','Unknown','Unknown',
       date '1900-01-01', date '9999-12-31', true