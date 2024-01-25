-- View with new column with time of day information
SELECT
	crime_id,
	created_at,
	updated_at,
	version,
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
	CASE
		WHEN extract(hour from date_of_occurrence) >= 0 AND extract(hour from date_of_occurrence) < 6 THEN 'night'
		WHEN extract(hour from date_of_occurrence) >= 6 AND extract(hour from date_of_occurrence) < 12 THEN 'morning'
		WHEN extract(hour from date_of_occurrence) >= 12 AND extract(hour from date_of_occurrence) < 18 THEN 'afternoon'
		WHEN extract(hour from date_of_occurrence) >= 18 AND extract(hour from date_of_occurrence) < 24 THEN 'evening'
		ELSE 'unknown'
	END AS time_of_day
FROM crime_data