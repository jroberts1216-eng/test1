CREATE OR REPLACE VIEW cldev.v_dim_total_parts_used_by_se AS
SELECT spu.se_serviceEventID AS serviceEventID,
    COUNT(se_partsUsedID) AS parts_used
FROM cldev.v_se_partsUsed AS spu
GROUP BY spu.se_serviceEventID WITH NO SCHEMA BINDING;