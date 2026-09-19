with source as (
    select * from {{ source('raw', 'weather_daily') }}
),

cleaned as (
    select
        city,
        date::date as weather_date,
        temperature_2m_max::numeric as temp_max_c,
        temperature_2m_min::numeric as temp_min_c,
        temperature_2m_mean::numeric as temp_mean_c,
        precipitation_sum::numeric as precipitation_mm,
        windspeed_10m_max::numeric as windspeed_max_kmh,
        loaded_at
    from source
)

select * from cleaned
