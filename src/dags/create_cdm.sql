drop table if exists cdm.dm_courier_ledger cascade;

create table cdm.dm_courier_ledger (
	id int4 NOT NULL GENERATED ALWAYS AS identity primary key,
	courier_id varchar NOT null, 
	courier_name varchar NOT null,
	settlement_year int NOT null,
	settlement_month int NOT null,
	orders_count numeric(14, 2) NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	rate_avg numeric NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_courier_ledger_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_rate_avg_check CHECK ((rate_avg >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);

