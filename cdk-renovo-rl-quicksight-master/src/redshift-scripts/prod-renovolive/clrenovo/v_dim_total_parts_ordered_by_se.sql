CREATE OR REPLACE VIEW clrenovo.v_dim_total_parts_ordered_by_se AS
WITH cte_quantities_by_poli AS (
    SELECT
        purchaseorderid,
        SUM(poli_qtyordered) AS qty_ordered,
        SUM(poli_qtyreceived) AS qty_received
    FROM clrenovo.v_po_lineItems AS pli
    GROUP BY purchaseorderid
)
SELECT
    c.serviceeventid AS serviceeventid,
    SUM(qty_ordered) AS qty_ordered,
    SUM(qty_received) AS qty_received
FROM cte_quantities_by_poli AS ppo
JOIN clrenovo.v_po_purchaseOrders AS c
    ON ppo.purchaseorderid = c.purchaseorderid
WHERE serviceeventid IS NOT NULL
GROUP BY serviceeventid WITH NO SCHEMA BINDING;