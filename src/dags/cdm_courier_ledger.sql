with courier_sum as (	
	select 	dc.id as id,
			dc.courier_id as courier_id,
			dc.courier_name as courier_name,
			extract (year from fd.order_ts) as settlement_year,
			extract (month from fd.order_ts) as settlement_month,
			count(fd.order_id) as orders_count,
			sum(fd.sum) as orders_total_sum,
			ROUND(AVG(fd.rate), 2) as rate_avg,
			sum(fd.tip_sum) as courier_tips_sum
	from dds.dm_couriers dc 
	inner join dds.fct_deliveries fd 
		on dc.id = fd.courier_id
	group by 	dc.id,
				dc.courier_id, 
				dc.courier_name,
				extract (year from fd.order_ts), 
				extract (month from fd.order_ts)
),
sum_rate as (select 
		dc.courier_name,
			SUM(CASE
    WHEN rate < 4 THEN GREATEST(fd.sum * 0.05, 100.00)
    WHEN rate < 4.5 THEN GREATEST(fd.sum * 0.07, 150.00)
    WHEN rate < 4.9 THEN GREATEST(fd.sum * 0.08, 175.00)
    ELSE GREATEST(fd.sum * 0.10, 200.00) end) as courier_order_sum 
from dds.fct_deliveries fd
inner join dds.dm_couriers dc 
on fd.courier_id = dc.id 
group by dc.courier_name),
all_courier_info as (
select	
		cs.courier_id,
		cs.courier_name,
		cs.settlement_year,
		cs.settlement_month,
		cs.orders_count,
		cs.orders_total_sum,
		cs.rate_avg,
		cs.orders_total_sum * 0.25 as order_processing_fee,
		cs.courier_tips_sum,
		sr.courier_order_sum,
		sr.courier_order_sum + cs.courier_tips_sum * 0.95 as courier_reward_sum 
from courier_sum as cs
inner join sum_rate sr
on cs.courier_name = sr.courier_name)
insert into cdm.dm_courier_ledger(
			courier_id,
			courier_name,
			settlement_year,
			settlement_month,
			orders_count,
			orders_total_sum,
			rate_avg,
			order_processing_fee,
			courier_order_sum,
			courier_tips_sum,
			courier_reward_sum
)
select
			courier_id,
			courier_name,
			settlement_year,
			settlement_month,
			orders_count,
			orders_total_sum,
			rate_avg,
			order_processing_fee,
			courier_tips_sum,
			courier_order_sum,
			courier_reward_sum
from all_courier_info
order by courier_name;
