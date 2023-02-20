INSERT INTO dds.dm_deliveries
(delivery_id)
SELECT 
	sd.delivery_id AS delivery_id
FROM 
	stg.deliveries AS sd
on conflict (delivery_id) do nothing;