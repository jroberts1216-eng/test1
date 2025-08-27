-- DROP MATERIALIZED VIEW  IF EXISTS "analytics"."dim_purchase_orders" cascade;

-- CREATE MATERIALIZED VIEW "analytics"."dim_purchase_orders" 
-- BACKUP NO 
-- --DISTSTYLE ALL 
-- DISTKEY ( DB,purchaseOrderID )
-- SORTKEY (  DB,purchaseOrderID )
-- AS

WITH facility_costcenter_clientsys_cte AS (
    SELECT 
        fc.db,
        fc.facilityID,
        fc.facility_costcenterid,
        fc.costCenter_name,
        fa.facility_name,
        fa.facility_abbrev,
        fa.isinactive AS facility_cs_isinactive,
        fa.isinactive AS facility_isinactive,
        fa.defaulterpregionid,
        cs.system_name,
        cs.systemID,
        c.clientid,
        c.regionid,
        cs.isinactive AS cs_isinactive,
        c.isinactive AS c_isinactive
    FROM unifieddb.v_facility_costCenters_spectrum AS fc 
	LEFT JOIN  unifieddb.v_facilities_spectrum AS fa ON fc.facilityID = fa.facilityID AND fc.DB = fa.DB
    LEFT JOIN  unifieddb.v_clientsystems_spectrum AS cs ON fa.systemid=cs.systemid AND fa.DB = cs.DB
    LEFT JOIN  unifieddb.v_clients_spectrum AS c ON cs.clientid=c.clientid AND fa.DB = cs.DB

 )

 , models_vnedors_devices_devices_tmp_cte AS (
    SELECT 
        model.DB,
        model.mtclModelID,
        model.mtclVendorID,
        model.mtclDeviceID,
        device.mtclDevice_name,
        vendor.mtVendor_name,
        model.mtclModel_name,
        upper(vendor.mtVendor_name) || ' - ' || model.mtclModel_name || ', ' || device.mtclDevice_name AS deviceClass,
        device.mtclDevice_name || ', ' || upper(vendor.mtVendor_name) || ' - ' || model.mtclModel_name AS equipmentDescription
    FROM unifieddb.v_mtclModels_spectrum AS model
    LEFT JOIN RenovoMaster.mtVendors_spectrum AS vendor ON  model.mtclVendorID = vendor.mtVendorID
    LEFT JOIN unifieddb.v_mtclDevices_spectrum AS device ON model.mtclDeviceID = device.mtclDeviceID AND model.DB = device.DB

 )


,  po_lineItems_CTES AS (
    SELECT
        poli.db,
        poli.purchaseorderid,
        MAX(CASE WHEN poli.poli_isBillable = 1 THEN 1 ELSE 0 END) AS hasBillable,
        MAX(CASE WHEN poli.poli_isBillable = 0 THEN 1 ELSE 0 END) AS hasNonBillable
    FROM
        unifieddb.v_po_lineItems_spectrum AS poli
    GROUP BY
        poli.db,
        poli.purchaseorderid
)
, po_billableState AS (
    SELECT
        db,
        purchaseorderid,
        CASE
            WHEN hasBillable = 0 AND hasNonBillable = 0 THEN 'None'
            WHEN hasBillable = 1 AND hasNonBillable = 1 THEN 'Mixed'
            WHEN hasBillable = 1 AND hasNonBillable = 0 THEN 'Billable'
            ELSE 'Non-Billable'
        END AS billableState
    FROM
        po_lineItems_CTES
)

, invoices_cte AS (
	select DB,purchaseOrderID, max(doc_created) AS invoice_uploaded from unifieddb.v_documents_spectrum where l_docTypeId=3 group by db, purchaseOrderID
	order by db, purchaseOrderID

)
SELECT
     ppo.*,
    CASE
        WHEN ppo.po_type = 0 THEN 'Svc Evt':: text
        WHEN ppo.po_type = 1 THEN 'VSC':: text
        ELSE 'Inventory':: text 
    END AS potypelabel,
    CASE
        WHEN ppo.po_closed IS NOT NULL THEN 'Closed':: text
        WHEN ppo.po_cancelled IS NOT NULL THEN 'Cancelled':: text
        WHEN ppo.po_received IS NOT NULL THEN 'Open-Received':: text
        WHEN ppo.po_orderplaced IS NOT NULL THEN 'Open-Ordered':: text
        ELSE 'Open':: text 
    END AS postatuslabel,
    CASE
        WHEN ppo.po_approvalstatus = 0 THEN 'Open for Changes':: text
        WHEN ppo.po_approvalstatus = 1 THEN 'Approval Pending':: text
        ELSE 'Approved':: text
    END AS poapprovalstatuslabel,
    ppo.po_created:: character varying(30):: timestamp without time zone AS po_created_label,
    asgn.usr_fullname AS assigned_user_fullname,
	COALESCE(sr.Name, sifcc.defaultErpRegionId, sefcc.defaultErpRegionId, sccfcc.defaultErpRegionId) AS client_businessRegion,
    CASE
        WHEN vendorpaymentsunapproved_cte.purchaseorderid IS NOT NULL THEN 'X':: text
        ELSE '':: text
    END AS vendorpaymentsunapproved,
    CASE
        WHEN (tot.po_total * 0.9) > total_no_cancelled_cte.total_amount THEN 'X':: text
        ELSE '':: text 
    END AS waitingvendorpayments,
    CASE 
        WHEN missingcredit_cte.purchaseorderid IS NOT NULL THEN 'X':: text
        ELSE '':: text 
    END AS missingcredit,

    btacte.billabletimeamt,
    bpacte.billablepartsamt,

    ppo.po_paymentterms AS netdays,
    tot.po_regular AS po_total_regular_value,
    tot.po_exchange AS po_total_exchange_value,
    tot.po_shipping AS po_total_shipping_value,
    tot.po_tax AS po_total_tax_value,
    tot.po_total AS po_total_value,
    tot.po_vsc_total AS po_vsc_total_value,
    COALESCE(sefcc.defaulterpregionid, sifcc.defaulterpregionid,sccfcc.defaulterpregionid) AS defaulterpregionid,
    COALESCE(sefcc.facility_cs_isinactive, sifcc.facility_cs_isinactive, sccfcc.facility_cs_isinactive) AS iscostcenterinactive,
    COALESCE(sefcc.facility_isinactive, sifcc.facility_isinactive, sccfcc.facility_isinactive) AS isfacilityinactive,
    COALESCE(sefcc.cs_isinactive, sifcc.cs_isinactive, sccfcc.cs_isinactive) AS issysteminactive,
    COALESCE(sefcc.c_isinactive, sifcc.c_isinactive, sccfcc.c_isinactive) AS iscientinactive,
    CASE
        WHEN COALESCE(sefcc.facility_cs_isinactive,sifcc.facility_cs_isinactive, sccfcc.facility_cs_isinactive) <> 1:: boolean 
        AND COALESCE(sefcc.facility_isinactive,sifcc.facility_isinactive, sccfcc.facility_isinactive) <> 1:: boolean
        AND COALESCE(sefcc.cs_isinactive,sifcc.cs_isinactive, sccfcc.cs_isinactive) <> 1:: boolean
        AND COALESCE(sefcc.c_isinactive,sifcc.c_isinactive, sccfcc.c_isinactive) <> 1:: boolean THEN 1
    ELSE 0 END AS isinactive,
	CASE
        WHEN sev.se_state = 1 THEN lu.name ||' (' || COALESCE(ses.seStat_name::text, 'Status Undefined') || ')'
		ELSE lu.name
	END AS seStatus,
    CASE 
		WHEN feq.facility_equipmentID is null THEN '(Inventory order)'
		WHEN feq.mtModelID is not null THEN mmod.equipmentDescription
		ELSE mtmp.mtclDevTmp_device	|| ', ' || upper(mtmp.mtclDevTmp_vendor) || ' - ' || mtmp.mtclDevTmp_model
	END AS equipmentDescription,
    COALESCE(sefcc.clientid, sifcc.clientid, sccfcc.clientid, NULL) AS client_id,
    COALESCE(sefcc.systemID, sifcc.systemID, sccfcc.systemID, NULL) AS system_id,
    COALESCE(sefcc.facilityID, sifcc.facilityID, sccfcc.facilityID, NULL) AS facility_id,
    COALESCE(sefcc.regionid, sifcc.regionid, sccfcc.regionid, NULL) AS region_id,
	COALESCE(sefcc.facility_costCenterID, sifcc.facility_costCenterID, sccfcc.facility_costCenterID, NULL) AS costCenterID,
	COALESCE(sefcc.systemID, sifcc.systemID, sccfcc.systemID, NULL) AS hospitalSystemID,
	v2.name AS vendorLocation,
	COALESCE(sefcc.system_name::text, sifcc.system_name::text, sccfcc.system_name::text, NULL) AS "system",
	COALESCE(sefcc.facility_name::text, sifcc.facility_name::text, sccfcc.facility_name::text, NULL) AS facility,
	COALESCE(sefcc.costCenter_name::text,  sifcc.costCenter_name::text, sccfcc.costCenter_name::text, NULL) AS costCenter,
    CASE
        WHEN sev.facility_equipmentID is not null THEN 
			CASE
                WHEN feq.mtModelID is not null THEN mmod.deviceClass
				ELSE 'TEMP: ' || UPPER(mtmp.mtclDevTmp_vendor) || ' - ' || mtmp.mtclDevTmp_model || ', ' || mtmp.mtclDevTmp_device
			END
		WHEN sev.ismSystemID is not null THEN
			CASE
                WHEN isys.mtModelID is not null THEN imod.deviceClass
				ELSE 'TEMP: ' || UPPER(itmp.mtclDevTmp_vendor) || ' - ' || itmp.mtclDevTmp_model || ', ' || itmp.mtclDevTmp_device
			END
	    ELSE 'Cost Center: ' || upper(sefcc.costCenter_name)
	END AS deviceClass,
    COALESCE(sefcc.facility_abbrev, sifcc.facility_abbrev, sccfcc.facility_abbrev, NULL) AS facAbbrev,
	asgn.usr_fullName AS poassigned,
	invoices_cte.invoice_uploaded,
	COALESCE(cte.billableState,'Non-Billable') AS billableState,
	COALESCE(mmod.mtclDevice_name, mtmp.mtclDevTmp_device, '') AS device,
	COALESCE(mmod.mtVendor_name, mtmp.mtclDevTmp_vendor, '') AS manufacturer,
	COALESCE(mmod.mtclModel_name, mtmp.mtclDevTmp_model, '') AS model,
    feq.equipment_number AS CETag,
	sev.se_workcompletedUTC AS se_completedUTC,
	COALESCE(sev.facility_equipmentID,feq.facility_equipmentID) AS facility_equipmentID,
	ppo.po_shipping IS NULL AS  ismissingShipTo,
	sev.se_deletedUTC IS NOT NULL AS is_sedeleted,
    sev.se_number AS Se_No,
    case
        when sev.facility_equipmentID is not null then
            feq.equipment_number + case when feq.equipment_serialNumber is not null AND feq.equipment_serialNumber <> ''
									then ' / ' + feq.equipment_serialNumber else '' end
								+ case when feq.equipment_altNumber is not null AND feq.equipment_altNumber <> ''
									then ' / ' + feq.equipment_altNumber else '' end
		when sev.ismSystemID is not null then isys.system_siteID
		else sccfcc.costCenter_name
    end AS EntityIdent,
    sev.ismSystemID,
    isys.system_siteID
FROM
    unifieddb.v_po_purchaseorders_spectrum_spectrum AS ppo
    LEFT JOIN unifieddb.v_se_serviceevents_spectrum AS sev ON ppo.serviceeventid = sev.serviceeventid   AND ppo.db = sev.db
    LEFT JOIN renovomaster.l_lookups_spectrum lu ON sev.se_state = lu.l_lookupid and lu.parentid =71
	LEFT JOIN unifieddb.v_facility_equipment_spectrum AS feq ON sev.facility_equipmentID = feq.facility_equipmentID AND sev.db = feq.db
		LEFT JOIN facility_costcenter_clientsys_cte sefcc ON  feq.facility_costCenterID = sefcc.facility_costCenterID AND feq.db = sefcc.db
        LEFT JOIN models_vnedors_devices_devices_tmp_cte mmod ON feq.mtModelID = mmod.mtclModelID AND feq.db =  mmod.DB
        LEFT JOIN unifieddb.v_mtclDeviceTemps_spectrum AS mtmp ON feq.mtclDeviceTempID = mtmp.mtclDeviceTempID AND feq.db =  mtmp.DB
    LEFT JOIN unifieddb.v_ismsystems_spectrum AS isys ON sev.ismsystemid = isys.ismsystemid AND sev.db = isys.db
        LEFT JOIN facility_costcenter_clientsys_cte sifcc ON isys.facility_costcenterid = sifcc.facility_costcenterid AND isys.db = sifcc.db
        LEFT JOIN models_vnedors_devices_devices_tmp_cte imod ON isys.mtmodelid = imod.mtclmodelid AND isys.db = imod.db
		LEFT JOIN unifieddb.v_mtclDeviceTemps_spectrum AS itmp ON isys.mtclDeviceTempID = itmp.mtclDeviceTempID AND isys.db =  itmp.DB
    LEFT JOIN facility_costcenter_clientsys_cte sccfcc ON sev.facility_costcenterid = sccfcc.facility_costcenterid    AND sev.db = sccfcc.db
    LEFT JOIN unifieddb.v_l_sestatus_spectrum AS ses ON sev.l_sestatusid = ses.sestatusid    AND sev.db = ses.db
    LEFT JOIN unifieddb.v_vendors2_spectrum AS v2 ON ppo.vendorid = v2.vendorid    AND ppo.db = v2.db
    LEFT JOIN renovomaster.v_users_spectrum AS asgn ON ppo.po_assigneduserid = asgn.userid  and lower(ppo.db) = lower(asgn.org_connectionname)
    LEFT JOIN renovomaster.sageregions_spectrum AS sr ON COALESCE(sifcc.defaultErpRegionId, sefcc.defaultErpRegionId, sccfcc.defaultErpRegionId ):: text = sr.id:: text
    LEFT JOIN po_billableState cte ON ppo.db = cte.db and ppo.purchaseOrderID = cte.purchaseOrderID
	LEFT JOIN invoices_cte invoices_cte ON ppo.db = invoices_cte.db and ppo.purchaseOrderID = invoices_cte.purchaseOrderID
    LEFT JOIN (
        SELECT
            po.db,
            po.purchaseorderid,
            sum(COALESCE(vi_cteregular.po_regular, 0:: numeric)) + sum(COALESCE(vi_cteexchange.po_regular, 0:: numeric)) AS po_regular,
            sum(COALESCE(vi_cteexchange.po_exchange, 0:: numeric)) AS po_exchange,
            sum(COALESCE(vi_ctevp.po_shipping, 0:: numeric)) AS po_shipping,
            sum(COALESCE(vi_ctevp.po_tax, 0:: numeric)) AS po_tax,
            sum(COALESCE(vi_cteregular.po_regular, 0:: numeric)) + sum(COALESCE(vi_cteexchange.po_regular, 0:: numeric)) - sum(COALESCE(vi_cteexchange.po_exchange, 0:: numeric)) + sum(COALESCE(vi_ctevp.po_shipping, 0:: numeric)) + sum(COALESCE(vi_ctevp.po_tax, 0:: numeric)) + sum(COALESCE(vi_ctevsc.vsc_total, 0:: numeric)) AS po_total,
            sum(COALESCE(vi_ctevsc.vsc_total, 0:: numeric)) AS po_vsc_total
        FROM
            unifieddb.v_po_purchaseorders_spectrum AS po
            LEFT JOIN (
                SELECT
                    poli.db,
                    poli.purchaseorderid,
                    COALESCE(
                        sum(poli.poli_amtunitregular * poli.poli_qtyordered),
                        0:: numeric
                    ) AS po_regular
                FROM
                    unifieddb.v_po_lineitems_spectrum AS poli
                WHERE
                    poli.poli_selreg = 1:: boolean
                GROUP BY
                    poli.db,
                    poli.purchaseorderid,
                    poli.poli_selreg
            ) vi_cteregular ON po.purchaseorderid = vi_cteregular.purchaseorderid
            AND po.DB = vi_cteregular.DB
            LEFT JOIN (
                SELECT
                    poli.DB,
                    poli.purchaseorderid,
                    CASE
                    WHEN poli.poli_selexch = 1:: boolean THEN COALESCE(
                        sum(poli.poli_amtunitexchange * poli.poli_qtyordered),
                        0:: numeric
                    )
                    ELSE 0:: numeric END AS po_regular,
                    CASE
                    WHEN poli.poli_selexch = 1:: boolean THEN COALESCE(
                        sum(poli.poli_amtunitexchcredit * poli.poli_qtyordered),
                        0:: numeric
                    )
                    ELSE 0:: numeric END AS po_exchange
                FROM
                    unifieddb.v_po_lineitems_spectrum AS poli
                WHERE
                    poli.poli_selreg = 0:: boolean
                GROUP BY
                    poli.db,
                    poli.purchaseorderid,
                    poli.poli_selexch
            ) vi_cteexchange ON po.purchaseorderid = vi_cteexchange.purchaseorderid
            and po.db = vi_cteexchange.db
            LEFT JOIN (
                SELECT
                    povp.db,
                    povp.purchaseorderid,
                    COALESCE(sum(povp.povp_shipping), 0:: numeric) AS po_shipping,
                    COALESCE(sum(povp.povp_taxother), 0:: numeric) AS po_tax
                FROM
                    unifieddb.v_po_vendorpayments_spectrum AS povp
                WHERE
                    povp.vendorinvoicestatus = 1
                    OR povp.vendorinvoicestatus = 3
                GROUP BY
                    povp.db,
                    povp.purchaseorderid
            ) vi_ctevp ON po.purchaseorderid = vi_ctevp.purchaseorderid
            AND po.DB = vi_ctevp.DB
            LEFT JOIN (
                SELECT
                    vsc.db,
                    vsc.purchaseorderid,
                    COALESCE(sum(vli.vscli_amount), 0:: numeric) AS vsc_total
                FROM
                    unifieddb.v_vs_vendorsubcontract_spectrum AS vsc
                    JOIN unifieddb.v_vsc_lineitems_spectrum AS vli ON vsc.vendorsubcontractid = vli.vendorsubcontractid
                    AND vsc.DB = vli.DB
                GROUP BY
                    vsc.db,
                    vsc.purchaseorderid
            ) vi_ctevsc ON po.purchaseorderid = vi_ctevsc.purchaseorderid
            AND po.DB = vi_ctevsc.DB
        GROUP BY
            po.DB,
            po.purchaseorderid
    ) tot ON ppo.purchaseorderid = tot.purchaseorderid
    AND ppo.DB = tot.DB
    LEFT JOIN (
        SELECT
            stm.db,
            stm.serviceeventid,
            COALESCE(sum(stm.setime_time * 100:: numeric), 0:: numeric) AS billabletimeamt
        FROM
            unifieddb.v_se_time_spectrum AS stm
            LEFT JOIN analytics.vm_orglookups lup ON stm.l_setimetypeid = lup.l_lookupid
            AND lup.kind_name:: text = 'SE Time Types':: text
            JOIN renovomaster.l_setimetypesext_spectrum tt ON tt.l_lookupid = lup.originalid
            AND tt.parentid = lup.parentid
            AND tt.billable = 1:: boolean
        GROUP BY
            stm.db,
            stm.serviceeventid
    ) btacte ON ppo.serviceeventid = btacte.serviceeventid
    and ppo.DB = btacte.DB
    LEFT JOIN (
        SELECT
            spu.DB,
            spu.se_serviceeventid,
            COALESCE(
                sum(spu.separtsused_qty * spu.separtsused_charge),
                0:: numeric
            ) AS billablepartsamt
        FROM
            unifieddb.v_se_partsused_spectrum AS spu
        WHERE
            spu.separtsused_billable = 1:: boolean
        GROUP BY
            spu.DB,
            spu.se_serviceeventid
    ) bpacte ON ppo.serviceeventid = bpacte.se_serviceeventid
    AND ppo.DB = bpacte.DB
    LEFT JOIN (
        SELECT
            povp.DB,
            povp.purchaseorderid,
            COALESCE(
                sum(
                    COALESCE(povp.povp_amount, 0:: numeric) + COALESCE(povp.povp_taxother, 0:: numeric) + COALESCE(povp.povp_shipping, 0:: numeric)
                ),
                0:: numeric
            ) AS total_amount
        FROM
            unifieddb.v_po_vendorpayments_spectrum AS povp
        WHERE
            povp.povp_cancelled IS NULL
        GROUP BY
            povp.DB,
            povp.purchaseorderid
    ) total_no_cancelled_cte ON ppo.purchaseorderid = total_no_cancelled_cte.purchaseorderid
    AND ppo.DB = total_no_cancelled_cte.DB
    LEFT JOIN (
        SELECT
            povp.db,
            povp.purchaseorderid
        FROM
            unifieddb.v_po_vendorpayments_spectrum AS povp
        WHERE povp.povp_cancelled IS NULL AND povp.povp_approved IS NULL
        GROUP BY povp.db,povp.purchaseorderid
    ) vendorpaymentsunapproved_cte ON ppo.purchaseorderid = vendorpaymentsunapproved_cte.purchaseorderid
    AND ppo.db = vendorpaymentsunapproved_cte.db
    LEFT JOIN (
        SELECT
            DISTINCT poli.db,
            poli.purchaseorderid
        FROM
            unifieddb.v_po_lineitems_spectrum AS poli
            LEFT JOIN unifieddb.v_po_vplineitems_spectrum AS vpli ON poli.polineitemid = vpli.polineitemid
            AND poli.db = vpli.db
            LEFT JOIN unifieddb.v_po_vendorpayments_spectrum AS povp ON vpli.povendorpaymentid = povp.povendorpaymentid
            AND povp.db = povp.db
            AND povp.povp_cancelled IS NULL
            AND povp.povp_approved IS NULL
        WHERE
            poli.poli_amtunitexchcredit > 0:: numeric(19, 4)
            AND povp.povendorpaymentid IS NULL
    ) missingcredit_cte ON ppo.purchaseorderid = missingcredit_cte.purchaseorderid
    AND ppo.db = missingcredit_cte.db
;