create schema if not exists sa;

/* ---------------------------------------------------------------------- */
/* --- Историческая ненормализированная таблица с партициями по годам --- */
/* ---------------------------------------------------------------------- */

create schema if not exists ha;

create table if not exists ha.earthquakes (
    "time" timestamptz,
    latitude real,
    longitude real,
    depth real,
    mag real,
    mag_type varchar(255),
    nst real,
    gap real,
    dmin real,
    rms real,
    net varchar(255),
    id varchar(255),
    updated timestamptz,
    place varchar(255),
    "type" varchar(255),
    horizontal_error real,
    depth_error real,
    mag_error real,
    mag_nst real,
    "status" varchar(255),
    location_source varchar(255),
    mag_source varchar(255),
    updated_at timestamptz default current_timestamp
) partition by range (time);

create table if not exists ha.earthquakes_old 
partition of ha.earthquakes
for values from ('1661-01-01') to ('1999-12-31');

create table if not exists ha.earthquakes_all_part
partition of ha.earthquakes
default;

/* ---------------------------------------------------------------------- */
/* --- В таблицах dim не содержатся отдельные сущности, это просто    --- */
/* --- продолжения таблицы fact. Разделено для аккуратности.          --- */
/* --- Главный ключ - id в таблице fact.earthquake_events.            --- */
/* --- Он varchar(255) и парсится из api.                             --- */
/* --- Также добавлены индексы.                                       --- */
/* --- Данные сюда парсятся начиная с 2000.01.01;                     --- */
/* --- все, что раньше - в ha.earthquakes.                            --- */
/* ---------------------------------------------------------------------- */

create schema if not exists fact;

create table if not exists fact.earthquake_events(
    id varchar(255) primary key,
    "type" varchar(255),
    "status" varchar(255),
    "time" timestamptz,
    mag real,
    mag_type varchar(255),
    updated_at timestamptz default current_timestamp
);

create index idx_earthquake_events_time on fact.earthquake_events(time);
create index idx_earthquake_events_mag on fact.earthquake_events(mag, mag_type);

create schema if not exists dim;

create table if not exists dim.earthquake_locations(
    event_id varchar(255) primary key references fact.earthquake_events(id),
    latitude real,
    longitude real,
    place varchar(255),
    updated_at timestamptz default current_timestamp
);

create index idx_earthquake_locations_lat_long on dim.earthquake_locations(latitude, longitude);

create table if not exists dim.earthquake_observations(
    event_id varchar(255) primary key references fact.earthquake_events(id),
    depth real,
    nst real,
    gap real,
    dmin real,
    rms real,
    horizontal_error real,
    depth_error real,
    mag_error real,
    mag_nst real,
    updated_at timestamptz default current_timestamp
);

create table if not exists dim.earthquake_sources(
    event_id varchar(255) primary key references fact.earthquake_events(id),
    net varchar(255),
    updated timestamptz,
    location_source varchar(255),
    mag_source varchar(255),
    updated_at timestamptz default current_timestamp
);
