select 1
from (select count(*) c from {{ ref('fact_violation') }}) f,
     (select count(*) c from (
         select distinct inspection_id, violation_code
         from {{ ref('int_restaurant_inspections_recovered') }}
         where inspection_id is not null and violation_code is not null) x) s
where f.c <> s.c