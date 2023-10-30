insert into ha.randomuser(
    gender,
    email,
    phone,
    cell,
    nat,
    name_title,
    name_first,
    name_last,
    dob_date,
    dob_age,
    registered_date,
    registered_age,
    id_name,
    id_value,
    picture_large,
    picture_medium,
    picture_thumbnail
)
select
    gender,
    email,
    phone,
    cell,
    nat,
    name_title,
    name_first,
    name_last,
    dob_date,
    dob_age,
    registered_date,
    registered_age,
    id_name,
    id_value,
    picture_large,
    picture_medium,
    picture_thumbnail
from sa.randomuser
on conflict (id_value) do update set
    gender = excluded.gender,
    email = excluded.email,
    phone = excluded.phone,
    cell = excluded.cell,
    nat = excluded.nat,
    name_title = excluded.name_title,
    name_first = excluded.name_first,
    name_last = excluded.name_last,
    dob_date = excluded.dob_date,
    dob_age = excluded.dob_age,
    registered_date = excluded.registered_date,
    registered_age = excluded.registered_age,
    id_name = excluded.id_name,
    picture_large = excluded.picture_large,
    picture_medium = excluded.picture_medium,
    picture_thumbnail = excluded.picture_thumbnail;

--user_id - это поле в sa, в которое копируется (id serial pkey) из ha
update sa.randomuser
set user_id = ha.randomuser.id
from ha.randomuser
where ha.randomuser.id_value = sa.randomuser.id_value;

insert into ha.randomuser_location(
    user_id,
    street_number,
    street_name,
    city,
    "state",
    country,
    postcode,
    coordinates_latitude,
    coordinates_longitude,
    timezone_offset,
    timezone_description
)
select
    user_id,
    location_street_number as street_number,
    location_street_name as street_name,
    location_city as city,
    location_state as "state",
    location_country as country,
    location_postcode as postcode,
    location_coordinates_latitude as coordinates_latitude,
    location_coordinates_longitude as coordinates_longitude,
    location_timezone_offset as timezone_offset,
    location_timezone_description as timezone_description
from sa.randomuser
on conflict (user_id) do update set
    street_number = excluded.street_number,
    street_name = excluded.street_name,
    city = excluded.city,
    "state" = excluded.state,
    country = excluded.country,
    postcode = excluded.postcode,
    coordinates_latitude = excluded.coordinates_latitude,
    coordinates_longitude = excluded.coordinates_longitude,
    timezone_offset = excluded.timezone_offset,
    "timezone_description" = excluded.timezone_description;

insert into ha.randomuser_login(
    user_id,
    uuid,
    username,
    "password",
    salt,
    md5,
    sha1,
    sha256
)
select
    user_id,
    login_uuid as uuid,
    login_username as username,
    login_password as "password",
    login_salt as salt,
    login_md5 as md5,
    login_sha1 as sha1,
    login_sha256 as sha256
from sa.randomuser
on conflict (user_id) do update set
    uuid = excluded.uuid,
    username = excluded.username,
    "password" = excluded.password,
    salt = excluded.salt,
    md5 = excluded.md5,
    sha1 = excluded.sha1,
    sha256 = excluded.sha256;