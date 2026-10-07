with v as (
    select distinct on (inspection_id, violation_code)
        inspection_id, restaurant_business_id, inspection_datetime,
        violation_code, violation_key
    from {{ ref('int_inspection_lines') }}
    where violation_code is not null
    order by inspection_id, violation_code, record_datetime desc nulls last
)
select
    md5(v.inspection_id || v.violation_code)        as violation_row_id,
    v.inspection_id,
    coalesce(r.restaurant_key,'-1')                 as restaurant_key,
    coalesce(loc.location_key,'-1')                 as location_key,
    to_char(v.inspection_datetime,'YYYYMMDD')::int  as inspection_date_key,
    v.violation_key,
    1                                               as violation_count
from v
left join {{ ref('dim_restaurant') }} r
  on r.restaurant_business_id = v.restaurant_business_id
 and v.inspection_datetime::date >= r.effective_date
 and v.inspection_datetime::date <  r.end_date
left join {{ ref('int_location') }} loc
  on loc.restaurant_business_id = v.restaurant_business_id