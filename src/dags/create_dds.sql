drop table if exists dds.dm_couriers cascade;

create table dds.dm_couriers (
id serial NOT null primary key,
courier_id varchar NOT null UNIQUE,
courier_name varchar NOT null
);

drop table if exists dds.dm_deliveries cascade;

create table dds.dm_deliveries (
id serial NOT null primary key ,
delivery_id varchar NOT null UNIQUE
);

drop table if exists dds.dm_orders cascade;

CREATE TABLE IF NOT EXISTS dds.dm_orders(
	order_id serial PRIMARY key,
	order_key	varchar(30) UNIQUE
);

drop table if exists dds.fct_deliveries cascade;

create table dds.fct_deliveries (
id serial NOT null primary key,
order_id int4 NOT null,
order_ts timestamp NOT null,
delivery_id int4 NOT null,
courier_id int4 NOT null,
address varchar NOT null,
delivery_ts timestamp NOT null,
rate int  NOT null,
sum numeric (14, 2) NOT null,
tip_sum numeric (14, 2) NOT null,
CONSTRAINT fct_deliveries_sum_check CHECK ((sum >= (0)::numeric)),
CONSTRAINT fct_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric)),
CONSTRAINT fct_deliveries_rate_check CHECK ((rate >= 1) and (rate <= 5)),
CONSTRAINT fct_deliveries_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(order_id),
CONSTRAINT fct_deliveries_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
CONSTRAINT fct_deliveries_delivery_id_fk FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id)
);

