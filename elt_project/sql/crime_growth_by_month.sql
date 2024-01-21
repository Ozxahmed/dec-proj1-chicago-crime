-- shows monthly crime growth (as percentage)
with crimes_by_month as (
	select 
		count(c.crime_id) as crime_amount,
		substring(c.date_of_occurrence,0,8) as calendar_month
	from 
		crime c
	inner join 
		date d
	on 
		substring(c.date_of_occurrence,0,11)=substring(d.date,0,11)
	group by
		substring(c.date_of_occurrence,0,8)
),
lag_crimes_by_month as (
	select
		calendar_month,
		crime_amount,
		lag(crime_amount) over (order by calendar_month) as prev_month_crime_amount
	from crimes_by_month
)
select
	*,
	crime_amount - prev_month_crime_amount as diff,
	to_char(((crime_amount - prev_month_crime_amount)/cast(prev_month_crime_amount as decimal)*100), 'fm00D00%') as crime_growth
from lag_crimes_by_month;