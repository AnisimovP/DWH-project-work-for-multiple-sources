INSERT INTO dds.dm_deliveries
(delivery_id)
SELECT 
	sd.delivery_id AS delivery_id
FROM 
	stg.deliveries AS sd
LEFT JOIN
	dds.dm_deliveries AS dd
		ON sd.delivery_id  =  dd.delivery_id 
WHERE 
	sd.order_ts::date BETWEEN '{{yesterday_ds}}' AND '{{ds}}'
	AND dd.delivery_id IS NULL;