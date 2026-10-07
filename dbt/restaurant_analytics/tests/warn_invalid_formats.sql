{{ config(severity='warn', store_failures=true) }}
select restaurant_business_id, 'building_identification_number' as field,
       building_identification_number as value
from {{ ref('int_restaurant_inspections_recovered') }}
where building_identification_number is not null
  and building_identification_number !~ '^[1-5][0-9]{6}$'
union all
select restaurant_business_id, 'borough_block_lot', borough_block_lot
from {{ ref('int_restaurant_inspections_recovered') }}
where borough_block_lot is not null and borough_block_lot !~ '^[1-5][0-9]{9}$'
union all
select restaurant_business_id, 'zip_code', zip_code
from {{ ref('int_restaurant_inspections_recovered') }}
where zip_code is not null and zip_code !~ '^[0-9]{5}$'