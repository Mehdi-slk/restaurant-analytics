select 1
from (select count(*) c from {{ ref('fact_inspection') }}) f,
     (select count(distinct inspection_id) c
      from {{ ref('int_restaurant_inspections_recovered') }}
      where inspection_id is not null) s
where f.c <> s.c