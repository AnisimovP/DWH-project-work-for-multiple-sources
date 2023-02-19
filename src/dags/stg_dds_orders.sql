INSERT INTO dds.dm_orders
(order_key)
SELECT 
	sd.order_id AS order_key
FROM 
	stg.deliveries AS sd
LEFT JOIN
	dds.dm_orders AS ddo
		ON sd.order_id = ddo.order_key
WHERE 
	sd.order_ts::date BETWEEN '{{yesterday_ds}}' AND '{{ds}}'
	AND ddo.order_key IS NULL;