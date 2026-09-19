with staged as (
    select * from {{ ref('stg_weather') }}
)

select
    city,
    weather_date,
    round(temp_max_c::numeric, 1) as temp_max_c,
    round(temp_min_c::numeric, 1) as temp_min_c,
    round(temp_mean_c::numeric, 1) as temp_mean_c,
    round((temp_max_c - temp_min_c)::numeric, 1) as temp_range_c,
    round(precipitation_mm::numeric, 1) as precipitation_mm,
    round(windspeed_max_kmh::numeric, 1) as windspeed_max_kmh,
    case
        when precipitation_mm > 0 then true
        else false
    end as had_precipitation
from staged
