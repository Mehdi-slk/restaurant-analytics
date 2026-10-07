select
    inspection_id, restaurant_business_id,
    inspection_datetime, grade_datetime, record_datetime,
    inspection_score, violation_code,
    {{ dbt_utils.generate_surrogate_key([
        "coalesce(inspection_type,'Unknown')",
        "coalesce(inspection_action,'Unknown')",
        "coalesce(inspection_grade,'Not Graded')"]) }}  as profile_key,
    coalesce(inspection_type,'Unknown')                 as inspection_type,
    coalesce(inspection_action,'Unknown')               as inspection_action,
    coalesce(inspection_grade,'Not Graded')             as inspection_grade,
    case when violation_code is not null
         then {{ dbt_utils.generate_surrogate_key(['violation_code']) }} end as violation_key
from {{ ref('int_restaurant_inspections_recovered') }}
where inspection_id is not null