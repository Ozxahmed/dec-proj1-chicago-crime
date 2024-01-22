-- shows two of the most popular crime types, crime amount totals, and the rank in total crimes for each ward (legislative district)
with dist_popular_crimes as (
	select distinct
		w.ward,
		w.alderman,
		first_value(c.primary_description) over(
			partition by w.ward
			order by count(c.crime_id) desc
			rows between unbounded preceding and unbounded following
			) as most_popular_crime,
		nth_value(c.primary_description, 2) over(
			partition by w.ward
			order by count(c.crime_id) desc
			rows between unbounded preceding and unbounded following
			) as second_most_popular_crime
	from 
		ward_offices w
	inner join 
		crime_data c 
	on 
		c.ward=w.ward
	group by
		w.ward,
		c.primary_description
),
dist_total_crimes as (
	select 
		w.ward,
		count(c.crime_id) as total_crimes,
		dense_rank() over(order by count(c.crime_id) desc) as total_crimes_rank
	from 
		crime_data c
	inner join
		ward_offices w
	on 
		c.ward=w.ward
	group by
		w.ward	
)
select
	dpc.ward,
	dpc.alderman,
	dpc.most_popular_crime as dist_most_popular_crime,
	dpc.second_most_popular_crime as dist_second_most_popular_crime,
	dtc.total_crimes as dist_total_crimes,
	dtc.total_crimes_rank as dist_total_crimes_rank
from 
	dist_popular_crimes dpc
inner join
	dist_total_crimes dtc
on 
	dpc.ward=dtc.ward
order by 
	ward;