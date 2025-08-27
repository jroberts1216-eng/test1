CREATE OR REPLACE VIEW cldev.v_dim_po_numbers_by_se AS
SELECT serviceeventid AS serviceeventid,
    LISTAGG(po_number, ', ') WITHIN GROUP (
        ORDER BY po_number ASC
    ) AS po_number
FROM cldev.v_po_purchaseOrders
WHERE serviceeventID IS NOT NULL
    AND po_cancelled IS NULL
GROUP BY serviceeventid WITH NO SCHEMA BINDING;