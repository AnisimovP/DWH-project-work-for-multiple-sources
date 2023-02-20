INSERT INTO dds.dm_orders
(order_key)
SELECT 
	order_id AS order_key
FROM 
	stg.deliveries
ON CONFLICT (order_key) DO NOTHING;