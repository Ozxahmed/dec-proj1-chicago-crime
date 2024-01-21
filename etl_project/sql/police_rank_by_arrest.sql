-- rank police districts based on successful arrest ratio
with arrest_count as (
	select 
		p.district,
		p.district_name, 
		p.address,
		p.zip,
		p.phone,
		count(c.crime_id) as total_arrests
	from 
		police p
	inner join 
		crime c
	on 
		p.district =  cast(floor(c.beat/100.0) as varchar)
	where
		c.arrest='Y'
	group by
		p.district
    -- limit 5
),
total_count as (
	select 
		p.district,
		count(c.crime_id) as total_crimes
	from 
		police p
	inner join 
		crime c
	on 
		p.district =  cast(floor(c.beat/100.0) as varchar)
	group by
		p.district
    -- limit 5
)
select
	ac.district,
	ac.district_name,
	ac.address,
	ac.zip,
	ac.phone,
	ac.total_arrests,
	tc.total_crimes,
	to_char(100.0*ac.total_arrests/tc.total_crimes, '990D99%') as arrest_percentage,
	dense_rank() over (order by 10000*ac.total_arrests/tc.total_crimes desc) as arrest_ratio_rank,
	dense_rank() over (order by ac.total_arrests) as arrest_total_rank,
	dense_rank() over (order by tc.total_crimes) as total_crimes_rank
from 
	arrest_count ac
inner join 
	total_count tc
on 
	ac.district=tc.district