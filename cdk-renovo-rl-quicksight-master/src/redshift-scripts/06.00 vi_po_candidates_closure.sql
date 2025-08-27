DROP MATERIALIZED VIEW  IF EXISTS  analytics.vi_po_candidates_closure;

create MATERIALIZED view analytics.vi_po_candidates_closure
SORTKEY(DB,purchaseOrderID)
 AS

WITH 
 excludepos_cte AS (
    SELECT db, purchaseorderid 
    FROM unified_db.vendorInvoices
    WHERE (blockedDate IS NOT NULL OR disputedDate IS NOT NULL) AND purchaseOrderId IS NOT NULL
)
, includeonlypos_cte AS (
	SELECT
        po.DB
        , po.purchaseOrderID
		, COUNT(DISTINCT poli.poLineItemID) as cnt1
		, COUNT(DISTINCT  poli.vendorInvoiceLineItemID) as cnt2
	FROM unified_db.po_purchaseOrders po 
	JOIN unified_db.po_vendorPayments povp ON povp.purchaseOrderID = po.purchaseOrderID AND povp.DB = po.DB
	LEFT JOIN unified_db.po_lineItems poli ON poli.purchaseOrderID = po.purchaseOrderID AND poli.DB = po.DB
	GROUP BY po.DB, po.purchaseOrderID
)
, raw_cte AS (
    SELECT
        ppo.DB
        ,ppo.purchaseOrderID
        ,ppo.po_approvalStatus
        ,ppo.po_type
        ,ppo.po_orderPlaced
        ,ppo.po_total_value AS po_total
        ,ppo.po_paidByCreditCard
        ,ppo.po_cod
        --,1 AS canClose
        ,ppo.po_number
        ,ppo.po_description	
    from analytics.dim_purchase_orders ppo
        LEFT JOIN excludepos_cte ecte ON ecte.db = ppo.DB AND ecte.purchaseOrderID = ppo.purchaseOrderID
        LEFT JOIN includeonlypos_cte icte ON icte.DB = ppo.DB AND icte.purchaseOrderId = ppo.purchaseOrderId
    where ecte.purchaseOrderID IS NULL 
        AND icte.purchaseOrderId IS NOT NULL AND icte.cnt1 = icte.cnt2
        AND (ppo.po_closed is null AND ppo.po_cancelled is null ) -- Open, non-cancelled POs
        AND (ppo.po_approvalStatus = 2 AND (po_orderPlaced is not null OR ppo.po_type = 1 )) -- Order must be placed (not used on type 1 - VSC)
        AND ppo.iscientinactive = 0	 AND	ppo.issysteminactive = 0 AND ppo.isfacilityinactive = 0 AND ppo.iscostcenterinactive = 0

)
, parts_source_cte AS (
    SELECT
        poli.DB
        ,poli.purchaseOrderID
        ,poli.poLineItemID
        ,ii.InventoryItemID
        ,poli.poli_qtyOrdered
        ,poli.poli_qtyReceived
        ,0 qtyUsed
        ,0 qtyInv
    FROM raw_cte pos
        join unified_db.po_lineItems poli on pos.purchaseOrderID = poli.purchaseOrderID and pos.DB = poli.DB
            join unified_db.inventoryItems ii on poli.polineitemid = ii.polineitemid and poli.DB = ii.DB
    WHERE not exists (select 1 from excludepos_cte ep where ep.purchaseOrderId = pos.purchaseOrderId and ep.DB = pos.DB)
	    AND exists (select 1 from includeonlypos_cte iop where iop.purchaseOrderId = poli.purchaseOrderId and iop.DB = poli.DB)
)
, parts_agg_cte AS (
    SELECT parts.DB,parts.poLineItemID, ii.InventoryItemID, ii.QuantityAvailable as qtyInv, SUM(spu.sePartsUsed_qty) qtyUsed
	FROM parts_source_cte parts
		JOIN unified_db.InventoryItems ii ON parts.InventoryItemID = ii.InventoryItemID and parts.DB = ii.DB
			JOIN unified_db.se_partsUsed spu ON ii.InventoryItemID = spu.InventoryItemID and ii.DB = spu.DB
	GROUP BY parts.DB,parts.poLineItemID, ii.InventoryItemID, ii.QuantityAvailable
)
, parts_cte AS (
    select
        DISTINCT 
        parts.DB
        , parts.purchaseOrderID
        , parts.poLineItemID
        , parts.InventoryItemID
        , parts.poli_qtyOrdered as qtyOrdered
        , parts.poli_qtyReceived as qtyRcvd
        , CASE 
            WHEN parts.InventoryItemID = cte.InventoryItemID AND parts.DB = cte.DB THEN cte.qtyUsed
            ELSE parts.qtyUsed
        END as qtyUsed
        , CASE 
            WHEN parts.InventoryItemID = cte.InventoryItemID AND parts.DB = cte.DB THEN cte.qtyInv
            ELSE parts.qtyInv
        END as qtyInv
    FROM parts_source_cte parts 
    LEFT JOIN parts_agg_cte cte ON parts.DB = cte.DB  and parts.InventoryItemID = cte.InventoryItemID

)

, exchanges_cte	AS (
    select 
        poli.DB
        ,poli.purchaseOrderID
        ,lie.poliex_returned
        ,lie.poliex_noCreditExch
        ,lie.polix_creditReceived
        ,lie.poliex_creditRcvInvoiceNo
    from unified_db.po_liExchanges lie
	join unified_db.po_lineItems poli on lie.poLineItemID = poli.poLineItemID and lie.DB = poli.DB
    where exists (select 1 from raw_cte p where p.purchaseOrderId = poli.purchaseOrderId and p.DB = poli.DB)
    AND not exists (select 1 from excludepos_cte ep where ep.purchaseOrderId = poli.purchaseOrderId and ep.DB = poli.DB)
	AND exists (select 1 from includeonlypos_cte iop where iop.purchaseOrderId = poli.purchaseOrderId and iop.DB = poli.DB)

)
, vps_cte AS (
    select
        ppo.DB
        ,ppo.purchaseOrderID
        ,povp.poVendorPaymentID
        ,COALESCE(povp.povp_shipping::int,0) + COALESCE(povp.povp_taxOther::int,0)						-- VP tax/other/shipping
            + (sum(COALESCE(povpli.povpli_liTot::int,0)) - sum(COALESCE(povplia.vpliAdj_amount::int,0)))	-- Sum VP detail costs
         AS  vpTotal
        ,case when povp.povp_approved is not null 
            then 1
            else 0
        end vpClosed
    from unified_db.po_purchaseOrders ppo
        left join unified_db.po_vendorPayments povp on ppo.purchaseOrderID = povp.purchaseOrderID AND ppo.DB = povp.DB
        left join unified_db.po_vpLineItems povpli on povp.poVendorPaymentID = povpli.poVendorPaymentID AND povp.DB = povpli.DB
        left join unified_db.po_vpLiAdjustments povplia on povpli.poVpLineItemID = povplia.poVpLineItemID AND povpli.DB = povplia.DB
    where exists (select 1 from raw_cte p where p.purchaseOrderId = ppo.purchaseOrderId and p.DB = ppo.DB)
    and not exists (select 1 from excludepos_cte ep where ep.purchaseOrderId = ppo.purchaseOrderId and ep.DB = ppo.DB)
    and exists (select 1 from includeonlypos_cte iop where iop.purchaseOrderId = ppo.purchaseOrderId and iop.DB = ppo.DB)
    group by 
        ppo.DB      ,ppo.purchaseOrderID
        ,povp.poVendorPaymentID        ,povp.povp_shipping
        ,povp.povp_taxOther        ,povp.povp_approved
)
, poVpTotal_cte AS (
    SELECT 
        vps.DB
        ,vps.purchaseOrderID
        ,SUM(vps.vpTotal) vpTotal
    FROM vps_cte vps
    GROUP BY vps.DB, vps.purchaseOrderID
)
, int_cte as (
    SELECT 
        DISTINCT
         raw_cte.*
        , CASE 
            WHEN parts_cte.qtyOrdered <> parts_cte.qtyRcvd	OR 	parts_cte.qtyOrdered <> (parts_cte.qtyUsed + parts_cte.qtyInv) THEN 0
            WHEN exc.poliex_returned IS NULL OR ( COALESCE(exc.poliex_noCreditExch::int,0) = 0 AND (exc.polix_creditReceived IS NULL OR exc.poliex_creditRcvInvoiceNo IS NULL)) THEN 0
            WHEN (raw_cte.po_paidByCreditCard = 0 AND raw_cte.po_cod= 0 ) AND COALESCE(povp_cte.vpTotal::int,0) < (COALESCE(raw_cte.po_total::int,0) * .9) THEN 0
            WHEN vps.purchaseOrderID is not null THEN 0
            ELSE 1
          END as canClose
        , vps.vpTotal

    FROM raw_cte raw_cte
    LEFT JOIN parts_cte ON raw_cte.DB = parts_cte.DB  AND raw_cte.purchaseOrderID = parts_cte.purchaseOrderID
    LEFT JOIN exchanges_cte exc ON raw_cte.DB = exc.DB AND raw_cte.purchaseOrderID = exc.purchaseOrderID
    LEFT JOIN poVpTotal_cte povp_cte ON raw_cte.DB = povp_cte.DB  AND raw_cte.purchaseOrderID = povp_cte.purchaseOrderID
    LEFT JOIN vps_cte vps ON  raw_cte.DB = vps.DB  AND raw_cte.purchaseOrderID = vps.purchaseOrderID AND vps.vpClosed = 0

)
, facility_cte AS (
	SELECT 
		vsc.DB,
		vsc.purchaseOrderID,
		vsFac.facility_name
	FROM unified_db.vs_vendorSubContract vsc
		join unified_db.vsc_lineItems vli on vsc.vendorSubContractID = vli.vendorSubContractID AND vsc.DB = vli.DB
			join unified_db.vsc_liEquipment vlie on vli.vscLineItemID = vlie.vscLineItemID AND vli.DB = vlie.DB
				join unified_db.facility_equipment vsFeq on vlie.facility_equipmentID = vsFeq.facility_equipmentID AND vlie.DB = vsfeq.DB
					join unified_db.facilities vsFac on vsFeq.facilityID = vsFac.facilityID AND vsFeq.DB = vsFac.DB
	where vsc.purchaseOrderID is not null
	GROUP BY vsc.DB,vsc.purchaseOrderID,vsFac.facility_name
)
, final_cte AS (
select  
	  pos.DB
    ,pos.purchaseOrderID
	,pos.po_number					
	,pos.po_description	
	,pos.po_type as po_type_id
	,case 
		when pos.po_type::int = 0 then 'Service Event'
		when pos.po_type::int = 1 then 'Vendor Sub-Contract'
		when pos.po_type::int = 2 then 'Parts for Inventory'
		else '?' 
	 end as po_type 
	,pos.po_total
	,COALESCE(pos.vpTotal,0) as vpTotal
	,pos.po_cod
	,pos.po_paidByCreditCard
	,case
		when pos.po_type::int = 0 then 
			case 
				when sev.facility_equipmentID is not null then 	
					case
						when feq.mtModelID is not null then mdev.mtclDevice_name || ', ' || upper(mven.mtVendor_name) || ' - ' || mmod.mtclModel_name
						else mtmp.mtclDevTmp_device || ', ' || upper(mtmp.mtclDevTmp_vendor) || ' - ' || mtmp.mtclDevTmp_model
					end
				when sev.ismSystemID is not null then 	 	
					case
						when isys.mtModelID is not null then idev.mtclDevice_name || ', ' || upper(iven.mtVendor_name) || ' - ' || imod.mtclModel_name
						else itmp.mtclDevTmp_device || ', ' || upper(itmp.mtclDevTmp_vendor) || ' - ' || itmp.mtclDevTmp_model
					end	
				else 'Cost Center: ' + fcc.costCenter_name
			end
		else 'N/A'
	 end deviceClass
	,case 
		when pos.po_type::int = 0
			then 
				case 
					when sev.facility_equipmentid is not null then efac.facility_name
					when sev.ismSystemID is not null then ifac.facility_name
					else fac.facility_name
				end
		when pos.po_type::int = 1 then facility_cte.facility_name
		else 'N/A'
		end facility
	,case when vscDates.vendorSubContractID is null 
		then ''
		else vscDates.vsc_start::TEXT --TO_CHAR(TO_TIMESTAMP(vscDates.vsc_start, 'YYYY-MM-DD HH:MI:SS'),'MM/DD/YYYY') -- convert(varchar(30),vscDates.vsc_start,109) 
	--			|| ' - ' || vscDates.vsc_end::TEXT --TO_CHAR(TO_TIMESTAMP(vscDates.vsc_end, 'YYYY-MM-DD HH:MI:SS'),'MM/DD/YYYY') -- convert(varchar(30),vscDates.vsc_end,109)
		end vscCoverage
	,ppo.po_created po_createdDate
	,u.user_fullname po_assignedTech
	,case 
			when sev.facility_equipmentid is not null 
				then efmru.user_fullname
			when sev.ismSystemID is not null 
				then ifmru.user_fullname
			else fmru.user_fullname
			end primaryManager
	/**/,cs.system_name
	,cs.systemid::text as system_id
	,c.clientid::text as client_id
	,coalesce(efac.facilityid,ifac.facilityid,fac.facilityid)::text as facility_id
	,r.region_name
	,r.regionid::text as region_id
from int_cte pos
	left join unified_db.po_purchaseOrders ppo on pos.purchaseOrderID = ppo.purchaseOrderID AND pos.DB = ppo.DB
		left join unified_db.se_serviceEvents sev on ppo.serviceEventID = sev.serviceEventID AND ppo.DB = sev.DB
			left join unified_db.facility_equipment feq on sev.facility_equipmentID = feq.facility_equipmentID AND sev.DB = feq.DB
				left join unified_db.mtclModels mmod on feq.mtModelID = mmod.mtclModelID AND feq.DB = mmod.DB
					left join RenovoMaster.mtVendors mven on mmod.mtclVendorID = mven.mtVendorID
					left join unified_db.mtclDevices mdev on mmod.mtclDeviceID = mdev.mtclDeviceID AND mmod.DB = mdev.DB
				left join unified_db.mtclDeviceTemps mtmp on feq.mtclDeviceTempID = mtmp.mtclDeviceTempID AND feq.DB = mtmp.DB
				left join unified_db.facility_costCenters efcc on feq.facility_costCenterID = efcc.facility_costCenterID AND feq.DB = efcc.DB
					left join unified_db.facilities efac on efcc.facilityID = efac.facilityID AND efcc.DB = efac.DB
						left join analytics.vFacilityManagementRoles efmr on efac.facilityID = efmr.facilityID AND efmr.isPrimaryManager::boolean=1 AND efmr.DB = pos.DB
							left join RenovoMaster.users efmru on efmr.userID = efmru.userID 
			left join unified_db.ismSystems isys on sev.ismSystemID = isys.ismSystemID AND sev.DB = isys.DB
				left join unified_db.mtclModels imod on feq.mtModelID = imod.mtclModelID AND feq.DB = imod.DB
					left join RenovoMaster.mtVendors iven on imod.mtclVendorID = iven.mtVendorID 
					left join unified_db.mtclDevices idev on imod.mtclDeviceID = idev.mtclDeviceID AND imod.DB = idev.DB
				left join unified_db.mtclDeviceTemps itmp on feq.mtclDeviceTempID = itmp.mtclDeviceTempID  AND feq.DB = itmp.DB
				left join unified_db.facility_costcenters ifcc on isys.facility_costCenterID = ifcc.facility_costCenterID AND isys.DB = ifcc.DB
					left join unified_db.facilities ifac on ifcc.facilityID = ifac.facilityID AND ifcc.DB = ifac.DB
						left join analytics.vFacilityManagementRoles ifmr on ifac.facilityID = ifmr.facilityID AND ifmr.isPrimaryManager::boolean=1 AND ifmr.DB = pos.DB
							left join RenovoMaster.users ifmru on ifmr.userID = ifmru.userID
			left join unified_db.facility_costCenters fcc on sev.facility_costCenterID = fcc.facility_costCenterID AND sev.DB = fcc.DB
				left join unified_db.facilities fac on fcc.facilityID = fac.facilityID AND fcc.DB = fac.DB
					left join analytics.vFacilityManagementRoles fmr on fac.facilityID = fmr.facilityID AND fmr.isPrimaryManager::boolean=1 AND fmr.DB = pos.DB
						left join RenovoMaster.users fmru on fmr.userID = fmru.userID 
		left join RenovoMaster.users u ON ppo.po_assignedUserID = u.userID
			left join unified_db.ClientSystems cs on cs.systemID = feq.systemID AND cs.DB = feq.DB
				left join unified_db.Clients c on c.clientID = cs.clientID AND c.DB = cs.DB
					left join unified_db.Regions r on c.regionID = r.regionID AND c.DB = r.DB
	left join unified_db.vs_vendorSubContract vscDates on ppo.purchaseOrderID = vscDates.purchaseOrderID AND ppo.DB = vscDates.DB
	left join facility_cte on ppo.purchaseOrderID = facility_cte.purchaseOrderID AND ppo.DB = facility_cte.DB
where canClose::int = 1 AND sev.se_deletedUTC IS NULL
)
SELECT * FROM final_cte;