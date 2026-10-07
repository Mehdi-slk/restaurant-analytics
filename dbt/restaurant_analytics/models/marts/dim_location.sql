select distinct
    location_key, borough, building_number, street_name, zip_code,
    community_board, council_district, census_tract,
    building_identification_number, borough_block_lot,
    neighborhood_tabulation_area, location_source
from {{ ref('int_location') }}
where location_key <> '-1'

union all
select '-1','Unknown','Unknown','Unknown','Unknown','Unknown',
       'Unknown','Unknown','Unknown','Unknown','Unknown','unknown'