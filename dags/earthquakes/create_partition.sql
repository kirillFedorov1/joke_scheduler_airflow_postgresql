create table if not exists ha.earthquakes_{{ execution_date.year }} 
partition of ha.earthquakes
for values from ('{{ execution_date.year }}-01-01')
    to ('{{ execution_date.year }}-12-31');
