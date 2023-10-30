create table if not exists ha.randomuser (
    id serial primary key,
    gender varchar(255),
    email varchar(255),
    phone varchar(255),
    cell varchar(255),
    nat varchar(255),
    name_title varchar(255),
    name_first varchar(255),
    name_last varchar(255),
    dob_date varchar(255),
    dob_age varchar(255),
    registered_date varchar(255),
    registered_age varchar(255),
    id_name varchar(255),
    id_value varchar(255) unique,
    picture_large varchar(255),
    picture_medium varchar(255),
    picture_thumbnail varchar(255),
    created_at timestamptz default current_timestamp
);

create table if not exists ha.randomuser_location (
    id serial primary key,
    user_id int unique,
    street_number varchar(255),
    street_name varchar(255),
    city varchar(255),
    "state" varchar(255),
    country varchar(255),
    postcode varchar(255),
    coordinates_latitude varchar(255),
    coordinates_longitude varchar(255),
    timezone_offset varchar(255),
    timezone_description varchar(255),
    created_at timestamptz default current_timestamp,
    foreign key (user_id) references ha.randomuser(id)
);

create table if not exists ha.randomuser_login (
    id serial primary key,
    user_id int unique,
    uuid varchar(255),
    username varchar(255),
    "password" varchar(255),
    salt varchar(255),
    md5 varchar(255),
    sha1 varchar(255),
    sha256 varchar(255),
    created_at timestamptz default current_timestamp,
    foreign key (user_id) references ha.randomuser(id)
);