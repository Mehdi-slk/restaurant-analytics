with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2036-01-01' as date)") }}
)
select
    to_char(date_day,'YYYYMMDD')::int          as date_key,
    date_day::date                             as full_date,
    extract(day from date_day)::int            as day,
    extract(month from date_day)::int          as month,
    to_char(date_day,'FMMonth')                as month_name,
    extract(quarter from date_day)::int        as quarter,
    extract(year from date_day)::int           as year,
    extract(isodow from date_day)::int         as day_of_week,
    to_char(date_day,'FMDay')                  as day_name,
    extract(isodow from date_day) in (6,7)     as is_weekend
from spine
union all
select -1, null, null, null, 'Unknown', null, null, null, 'Unknown', false