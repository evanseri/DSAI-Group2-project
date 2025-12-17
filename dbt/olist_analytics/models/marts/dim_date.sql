-- Standard BigQuery SQL to generate dates
with date_spine as (
  select 
    * from 
    unnest(generate_date_array('2016-01-01', '2020-01-01', interval 1 day)) as date_day
)

select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day,
  extract(dayofweek from date_day) as day_of_week,
  format_date('%B', date_day) as month_name,
  format_date('%A', date_day) as day_name,
  case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend
from date_spine