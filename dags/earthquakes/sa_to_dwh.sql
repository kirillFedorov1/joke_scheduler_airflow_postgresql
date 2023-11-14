insert into fact.earthquake_events
select
    id,
    "type",
    "status",
    cast("time" as timestamptz) as time,
    mag,
    "magType" as mag_type
from sa.earthquakes
on conflict (id) do update
set
    "type" = excluded."type",
    "status" = excluded."status",
    "time" = excluded.time,
    mag = excluded.mag,
    mag_type = excluded.mag_type,
    updated_at = now();

insert into dim.earthquake_locations
select
    id as event_id,
    latitude,
    longitude,
    place
from sa.earthquakes
on conflict (event_id) do update
set
    latitude = excluded.latitude,
    longitude = excluded.longitude,
    place = excluded.place,
    updated_at = now();

insert into dim.earthquake_observations
select
    id as event_id,
    depth,
    nst,
    gap,
    dmin,
    rms,
    "horizontalError" as horizontal_error,
    "depthError" as depth_error,
    "magError" as mag_error,
    "magNst" as mag_nst
from sa.earthquakes
on conflict (event_id) do update
set
    depth = excluded.depth,
    nst = excluded.nst,
    gap = excluded.gap,
    dmin = excluded.dmin,
    rms = excluded.rms,
    horizontal_error = excluded.horizontal_error,
    depth_error = excluded.depth_error,
    mag_error = excluded.mag_error,
    mag_nst = excluded.mag_nst,
    updated_at = now();

insert into dim.earthquake_sources
select
    id as event_id,
    net,
    cast(updated as timestamptz) as updated,
    "locationSource" as location_source,
    "magSource" as mag_source
from sa.earthquakes
on conflict (event_id) do update
set
    net = excluded.net,
    updated = excluded.updated,
    location_source = excluded.location_source,
    mag_source = excluded.mag_source,
    updated_at = now();