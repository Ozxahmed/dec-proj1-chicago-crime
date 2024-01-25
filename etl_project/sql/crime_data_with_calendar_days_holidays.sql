-- View which combines crime data with the date/calendar data to give us one table with both
SELECT
	crime_id,
	created_at,
	updated_at,
	"version",
	"case",
	date_of_occurrence,
	block,
	iucr,
	primary_description,
	secondary_description,
	location_description,
	arrest,
	domestic,
	beat,
	ward,
	fbi_cd,
	x_coordinate,
	y_coordinate,
	latitude,
	longitude,
	"year",
	"day",
	"month",
	day_of_week_name
	month_name,
	day_of_week,
	holiday_name
FROM crime_data c
left join date d on
	date(c.date_of_occurrence) = d.date