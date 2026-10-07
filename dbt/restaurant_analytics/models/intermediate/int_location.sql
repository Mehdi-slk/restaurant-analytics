with latest as (
    select distinct on (restaurant_business_id)
        restaurant_business_id, borough, building_number, street_name, zip_code,
        community_board, council_district, census_tract,
        building_identification_number, borough_block_lot, neighborhood_tabulation_area
    from {{ ref('int_restaurant_inspections_recovered') }}
    order by restaurant_business_id,
             record_datetime desc nulls last, inspection_datetime desc nulls last
),
filled as (
    select
        restaurant_business_id,
        coalesce(borough,'Unknown')                        as borough,
        coalesce(building_number,'Unknown')                as building_number,
        coalesce(street_name,'Unknown')                    as street_name,
        coalesce(zip_code,'Unknown')                       as zip_code,
        coalesce(community_board,'Unknown')                as community_board,
        coalesce(council_district,'Unknown')               as council_district,
        coalesce(census_tract,'Unknown')                   as census_tract,
        coalesce(building_identification_number,'Unknown') as building_identification_number,
        coalesce(borough_block_lot,'Unknown')              as borough_block_lot,
        coalesce(neighborhood_tabulation_area,'Unknown')   as neighborhood_tabulation_area
    from latest
),
counted as (
    select *,
        (borough='Unknown')::int + (building_number='Unknown')::int
      + (street_name='Unknown')::int + (zip_code='Unknown')::int
      + (community_board='Unknown')::int + (council_district='Unknown')::int
      + (census_tract='Unknown')::int + (building_identification_number='Unknown')::int
      + (borough_block_lot='Unknown')::int
      + (neighborhood_tabulation_area='Unknown')::int as unknown_count
    from filled
)
select
    restaurant_business_id,
    case when unknown_count = 10 then '-1'
         else {{ dbt_utils.generate_surrogate_key([
             'borough','building_number','street_name','zip_code','community_board',
             'council_district','census_tract','building_identification_number',
             'borough_block_lot','neighborhood_tabulation_area']) }} end as location_key,
    borough, building_number, street_name, zip_code, community_board,
    council_district, census_tract, building_identification_number,
    borough_block_lot, neighborhood_tabulation_area,
    case when unknown_count = 10 then 'unknown'
         when unknown_count > 0  then 'partial_unknown'
         else 'original' end as location_source
from counted