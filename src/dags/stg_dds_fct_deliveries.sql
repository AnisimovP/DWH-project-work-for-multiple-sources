INSERT INTO dds.fct_deliveries
(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
SELECT 
	ddo.order_id AS order_id,
	sd.order_ts AS order_ts,
	dd.id AS delivery_id,
	dc.id AS courier_id,
	sd.address AS address,
	sd.delivery_ts AS delivery_ts,
	sd.rate AS rate,
	sd.sum AS sum,
	sd.tip_sum AS tip_sum	
FROM 
	stg.deliveries AS sd
JOIN
	dds.dm_orders AS ddo 	
		ON sd.order_id = ddo.order_key
JOIN 	
	dds.dm_deliveries AS dd
		ON sd.delivery_id = dd.delivery_id
JOIN 
	dds.dm_couriers AS dc 
		ON sd.courier_id = dc.courier_id
ON CONFLICT (order_id) DO UPDATE SET
    order_ts = excluded.order_ts,
    address = excluded.address,
    delivery_ts = excluded.delivery_ts,
    rate = excluded.rate,
    sum = excluded.sum,
    tip_sum = excluded.tip_sum;