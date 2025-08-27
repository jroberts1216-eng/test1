drop MATERIALIZED IF EXISTS view analytics.vi_purchase_order; 
create MATERIALIZED view analytics.vi_purchase_order 
SORTKEY
    (DB, purchaseOrderID) 
	AS
WITH
 po_openall AS (
	select  
  	*
	from analytics.dim_purchase_orders ppo
	WHERE 
		-- ppo.po_type <> 1		AND ppo.po_cancelled is null		AND
		 is_sedeleted = FALSE
		AND (iscostcenterinactive <> 1 OR (PPO.po_Type <> 0))
		AND (isfacilityinactive <> 1 OR (PPO.po_Type <> 0))
		AND (issysteminactive <> 1 OR (PPO.po_Type <> 0))
		AND (iscientinactive <> 1 OR (PPO.po_Type <> 0))
)

, vendorInvoices_cte AS (
	SELECT 
		DB,
		purchaseOrderId ,
		MIN(completedApDate) as completedApDate
	FROM unified_db.vendorInvoices
	WHERE (invoiceStatus = 1 or invoiceStatus=8)
	AND completedApDate is not NULL 
	AND (approvedToPayFieldDate is null or invoiceStatus <> 3)
	GROUP BY DB, purchaseOrderID
)

, po_lineItems_cte AS (

	SELECT poli.* 
	FROM unified_db.po_lineItems poli
	JOIN po_openall o ON poli.purchaseOrderID = o.purchaseOrderID AND poli.db = o.db
	ORDER BY poli.db, poli.purchaseOrderID

)
, exchangesOpen_cte AS (   -- Can exchanges be why PO is still open
	select
		pli.DB
		,pli.purchaseOrderID
		,count(pliex.poLineItemID) as cntExchgOpen
	from unified_db.po_liExchanges pliex
		join po_lineItems_cte pli on pliex.poLineItemID = pli.poLineItemID
			and pliex.db = pli.db
	where 
			poliex_returned is null
			OR
			(poliex_rma is null OR poliex_rma = '')
			OR
			(poliex_trackingNo is null OR poliex_trackingNo = '')
			OR
			(polix_creditReceived is null AND poliex_noCreditExch = 0)
	group by pli.db,pli.purchaseOrderID
)
, partsNotRcvd_cte AS (
	select
		pli.DB,
		pli.purchaseOrderID,
		count(pli.poLineItemID) as cntPartsNotRcvd
	from po_lineItems_cte pli
	where pli.poli_qtyReceived < pli.poli_qtyOrdered
	group by pli.db,pli.purchaseOrderID
)
, po_openall_w_budget AS (
	SELECT 
		ppo.*,
		vendorInvoices_cte.completedApDate,
		cte.equipBudgetCoverage,
		cte.budgetAmount,
		CASE
			WHEN ISNULL(eo.cntExchgOpen, 0) = 0 THEN
				''
			ELSE
				'X'
		END AS exchgsOpen,
		CASE
			WHEN ISNULL(pnr.cntPartsNotRcvd, 0) = 0 THEN
				''
			ELSE
				'X'
		END AS partsNotReceived,
		CASE
			when vpcc.PurchaseOrderID IS NOT NULL THEN 'Ready To Close'
			else ''
		END as ready_to_close

	FROM po_openall ppo
	LEFT JOIN (
		SELECT 
			o.db,
			o.purchaseOrderID,
			cdt.coverage_coverageDetailsName as  equipBudgetCoverage,
			feb.budgetAmount as budgetAmount
		FROM	po_openall o
		JOIN	unified_db.se_serviceEvents sev ON o.serviceEventID = sev.serviceEventID AND o.DB = sev.DB
		JOIN	unified_db.facility_equipment feq on sev.facility_equipmentID = feq.facility_equipmentID AND sev.DB = feq.DB
		JOIN	unified_db.facility_equipmentBudget feb on feq.facility_equipmentID = feb.facility_equipmentID AND feq.DB = feb.DB
		JOIN	unified_db.coverage_coverageSpecification ccs ON feb.coverageSpecId = ccs.coverage_coverageSpecId AND feb.DB = ccs.DB
		LEFT JOIN unified_db.coverage_coverageDetailsTemplates cdt ON ccs.coverage_coverageDetailsId = cdt.coverage_coverageDetailsId AND ccs.DB = cdt.DB
		WHERE	feb.UpdatedBy_id IS NULL AND feb.clientApprovalDate IS NOT NULL AND	feb.CoverageStartDate <> feb.CoverageEndDate AND o.PO_created BETWEEN feb.CoverageStartDate AND feb.CoverageEndDate
	) cte
	ON  ppo.db = cte.db  AND ppo.purchaseOrderID = cte.purchaseOrderID
	LEFT JOIN exchangesOpen_cte eo ON ppo.db = eo.db  AND ppo.purchaseOrderID = eo.purchaseOrderID
	LEFT JOIN partsNotRcvd_cte AS pnr ON ppo.purchaseOrderID = pnr.purchaseOrderID and ppo.DB = pnr.DB
	LEFT JOIN vendorInvoices_cte on ppo.purchaseOrderID = vendorInvoices_cte.purchaseOrderID and ppo.DB = vendorInvoices_cte.DB
	LEFT JOIN analytics.vi_po_candidates_closure vpcc On ppo.purchaseOrderID =  vpcc.purchaseOrderID AND ppo.DB =  vpcc.DB 
)
SELECT * from po_openall_w_budget
;