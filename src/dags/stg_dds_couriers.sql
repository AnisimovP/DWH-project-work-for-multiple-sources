INSERT INTO dds.dm_couriers
(courier_id, courier_name)
SELECT 
	sc.courier_id, sc.courier_name
FROM stg.couriers sc
left join dds.dm_couriers dc 
	on sc.courier_id = dc.courier_id
	and sc.courier_name = dc.courier_name
where dc.courier_id is null
ON CONFLICT (courier_id) DO UPDATE SET
courier_name = EXCLUDED.courier_name;