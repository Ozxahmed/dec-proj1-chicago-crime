-- shows the average amount of crimes committed on a holiday date vs a non-holiday date
with crimes_by_day as (
	select 
		count(c.crime_id) as crimes_amount, 
		substring(d.date,0,11) as calendar_date,
		(case when d.holiday_name is not null
			then 'Y' else 'N'
		end) as holiday
	from 
		crime c
	inner join 
		date d
	on 
		substring(c.date_of_occurrence,0,11)=substring(d.date,0,11)
	group by 
		calendar_date, 
		holiday
)
select
	round(avg(crimes_amount),0) as average_crimes_count,
	holiday
from crimes_by_day
group by holiday