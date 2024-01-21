-- shows monthly crime growth (as percentage)
with crimes_by_month as (
	select 
		count(c.crime_id) as crime_amount,
		date_part('month',c.date_of_occurrence)  as calendar_month,
		date_part('year',c.date_of_occurrence)  as calendar_year
	from 
		crime c
	inner join 
		date d
	on 
		date(c.date_of_occurrence)=d.date
	group by
		date_part('month',c.date_of_occurrence), 
		date_part('year',c.date_of_occurrence)
),
lag_crimes_by_month as (
	select
		calendar_month,
		calendar_year,
		crime_amount,
		lag(crime_amount) over (order by calendar_month, calendar_year) as prev_month_crime_amount
	from crimes_by_month
)
select
	*,
	crime_amount - prev_month_crime_amount as diff,
	to_char(((crime_amount - prev_month_crime_amount)/cast(prev_month_crime_amount as decimal)*100),'990D99%') as crime_growth
from lag_crimes_by_month
order by 
	calendar_year,
	calendar_month