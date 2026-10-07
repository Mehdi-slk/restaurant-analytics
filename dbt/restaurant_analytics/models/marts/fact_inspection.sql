with insp as (
    select distinct on (inspection_id)
        inspection_id, restaurant_business_id, inspection_datetime,
        grade_datetime, record_datetime, inspection_score, profile_key
    from {{ ref('int_inspection_lines') }}
    order by inspection_id, record_datetime desc nulls last
)
select
    i.inspection_id,
    coalesce(r.restaurant_key,'-1')                         as restaurant_key,
    coalesce(loc.location_key,'-1')                         as location_key,
    to_char(i.inspection_datetime,'YYYYMMDD')::int          as inspection_date_key,
    coalesce(to_char(i.grade_datetime,'YYYYMMDD')::int, -1) as grade_date_key,
    i.profile_key,
    i.inspection_score
from insp i
left join {{ ref('dim_restaurant') }} r
  on r.restaurant_business_id = i.restaurant_business_id
 and i.inspection_datetime::date >= r.effective_date
 and i.inspection_datetime::date <  r.end_date
left join {{ ref('int_location') }} loc
  on loc.restaurant_business_id = i.restaurant_business_id