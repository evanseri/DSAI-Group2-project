with geolocation as (
    select * from {{ ref('stg_geolocation') }}
)

select
    zip_code,
    -- Calculate the center point of the zip code
    avg(latitude) as latitude,
    avg(longitude) as longitude,
    max(city) as city,
    max(state) as state
from geolocation
group by zip_code