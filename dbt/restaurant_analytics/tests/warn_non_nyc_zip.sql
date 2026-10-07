{{ config(severity='warn', store_failures=true) }}
select restaurant_business_id, zip_code
from {{ ref('int_restaurant_inspections_recovered') }}
where zip_code is not null
  and zip_code !~ '^(100|101|102|103|104|111|112|113|114|116|11004|11005)'