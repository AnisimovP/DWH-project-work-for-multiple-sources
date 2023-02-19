drop table if exists stg.deliveries cascade;

create table stg.deliveries (
id serial not null primary key,
order_id varchar not null,
order_ts timestamp not null,
delivery_id varchar not null,
courier_id varchar not null,
address text not null,
delivery_ts timestamp not null,
rate int not null,
sum int not null,
tip_sum int not null
);

drop table if exists stg.couriers cascade;

create table stg.couriers (
id serial not null primary key,
courier_id varchar not null,
courier_name text not null
);
