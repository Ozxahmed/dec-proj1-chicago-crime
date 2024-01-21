-- shows the average amount of crimes committed on a holiday date vs a non-holiday date
with crimes_by_day as (
	select 
		count(c.crime_id) as crimes_amount, 
		d.date as calendar_date,
		(case when d.holiday_name is not null
			then 'Y' else 'N'
		end) as holiday
	from 
		crime c
	inner join 
		date d
	on 
		date(c.date_of_occurrence)=d.date
	group by 
		calendar_date, 
		holiday
)
select
	round(avg(crimes_amount),2) as average_crimes_count,
	holiday
from crimes_by_day
group by holiday