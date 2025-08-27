DROP MATERIALIZED VIEW  IF EXISTS ANALYTICS.vi_dm_list;

CREATE MATERIALIZED VIEW ANALYTICS.vi_dm_list 
SORTKEY (  DB,serviceeventID )
AS

WITH 
cte_counts_source as (
    SELECT
		db,
		purchaseorderid,
		sum(poli_qtyordered) qty_ordered,
		sum(poli_qtyreceived) qty_received
    FROM
        unified_db.po_lineItems pli
	group by db,purchaseorderid

)
, cte_parts_counts AS (
select c.db,serviceeventid,sum(qty_ordered) as qty_ordered, sum(qty_received) as qty_received from cte_counts_source ppo 
join unified_db.po_purchaseorders c on ppo.db = c.db and ppo.purchaseorderid
= c.purchaseorderid
group by c.db,serviceeventid
)
, parts_used_cte AS (
    SELECT
        spu.DB,
        spu.se_serviceEventID as serviceEventID,
        COUNT(se_partsUsedID) as parts_used
	FROM unified_db.se_partsUsed AS spu
    GROUP BY spu.DB, spu.se_serviceEventID
)
, purchase_orders_serviceevents_ as (
    SELECT 
        DB,
        serviceeventid,
        LISTAGG( po_number , ', ') WITHIN GROUP (ORDER BY po_number ASC) as PO_Number
    FROM unified_db.po_purchaseOrders
    where serviceeventID is not null and po_cancelled is null
    GROUP BY DB, serviceeventid
)
, final_cte AS (
select 
	sev.DB
	,CASE 
		WHEN sev.facility_equipmentid is not null THEN ecs.systemid
		WHEN sev.ismsystemid is not null THEN ics.systemid
		ELSE ccs.systemid
	 END system_id
	,CASE
		WHEN sev.facility_equipmentid is not null THEN efac.facilityid
		WHEN sev.ismsystemid is not null THEN ifac.facilityid
		ELSE cfac.facilityid
	 END facility_id
	, CASE 
		WHEN sev.facility_equipmentid is not null THEN ecs.clientID
		WHEN sev.ismsystemid is not null THEN ics.clientID
		ELSE ccs.clientID
	 END client_id
	, CASE 
		WHEN sev.facility_equipmentid is not null THEN eccs.regionID
		WHEN sev.ismsystemid is not null THEN iccs.regionID
		ELSE cccs.regionID
	 END region_id
	, CASE 
		WHEN sev.facility_equipmentid is not null THEN ecs.system_name
		WHEN sev.ismsystemid is not null THEN ics.system_name
		ELSE ccs.system_name
	 END System_name
	, CASE
		WHEN sev.facility_equipmentid is not null THEN efac.facility_name
		WHEN sev.ismsystemid is not null THEN ifac.facility_name
		ELSE cfac.facility_name
	 END AS Facility
	, CASE 
		WHEN sev.facility_equipmentid is not null THEN efcc.costcenter_name
		WHEN sev.ismsystemid is not null THEN ifcc.costCenter_name
		ELSE cfcc.costcenter_name 
	 END CostCenter
	, se_callTakenUTC AS ReceivedDate
    , se_workCompletedUTC
    , sev.serviceeventID
	, sev.se_number Se_No
	, CASE 
		WHEN sev.se_state = 3 then 'Closed'
		WHEN sev.se_state = 2 THEN 'Completed'
		ELSE lss.seStat_name
	 END Status
	, CASE 
		WHEN sev.facility_equipmentid is not null THEN equipment_number														
		WHEN sev.ismSystemID is not null THEN isys.system_siteID
		ELSE 'CC: ' || cfcc.costcenter_name
	 END as CeTagNo
	, feq.equipment_SerialNumber as SerialNumber
	, feq.equipment_location as "Location"
	, feq.equipment_altNumber as FacAssetNumber
	, feq.IsMissionCritical
	, CASE 
		WHEN sev.facility_equipmentid is not null THEN 
			CASE
				WHEN feq.mtmodelid is not null THEN emtd.mtDevice_name
				ELSE emdt.mtclDevTmp_device
			END 
		WHEN sev.ismSystemID is not null THEN
			CASE
				WHEN isys.mtmodelid is not null THEN imtd.mtdevice_name 
				ELSE imdt.mtcldevtmp_device
			END
		ELSE ''
	 END																	Device
	,case 
		when sev.facility_equipmentid is not null 
			then
				case when feq.mtModelID is not null 
					then emtv.mtvendor_name 
					else emdt.mtclDevTmp_vendor
				end 
		when sev.ismSystemID is not null 
			then 
				case when isys.mtmodelid is not null
					then imtv.mtvendor_name
					else imdt.mtcldevTmp_vendor
				end
		else ''
	 end Manufacturer
	,case 
		when sev.facility_equipmentid is not null
			then 
				case when feq.mtmodelid is not null 
					then emtm.mtmodel_name
					else emdt.mtclDevTmp_model
				end 
		when sev.ismSystemID is not null 
			then 
				case when isys.mtModelID is not null 
					then imtm.mtModel_name
					else imdt.mtcldevtmp_model 
				end
			else ''
	 end as "Model"
	,case when sev.l_seInitiatedById = 5
		then 'SM'
		else 'DM'
	 end SMDM
     , COALESCE(sev.l_seSymptomID, 0) = 7 as Is_Incoming_Inspection
	 ,lpms.pmSched_name														SmSched
	 ,COALESCE(dim_se_assigned.se_assigned,'-- Unassigned --') 				Technician
	 ,po.po_number															PO_Number
	 ,ll.name																specialCondition
	 ,CASE 
	 	WHEN sev.se_completedUTC is null THEN 'Open'
		ELSE 'Closed'
	 END as SE_Status
     , sev.se_completedUTC
     , sev.se_sm::text as se__sm
     , sev.se_notes Notes1
	 , notes_cte.note2 Notes2
     --Hardcoded - 1 displays as Emergency Down; 2 = Urgent; 3 = Routine
     , CASE 
        WHEN sev.l_sePriorityID =  1 THEN 'Emergency Down'
        WHEN sev.l_sePriorityID =  2 THEN 'Urgent'
        WHEN sev.l_sePriorityID =  3 THEN 'Routine'
        ELSE ''
       END as Priority
     , TRIM(sev.se_problem) as problem
     , TRIM(sev.se_problemtext) as problem_text
	 , NVL(TRIM(sev.se_resolution),'') AS resolution
     , NVL(lrs.seRes_name,'') AS resolutionCode
	 , NVL(TRIM(lsm.seSympt_name),'') AS symptom
	 , NVL(TRIM(ldm.seDiagn_name),'') AS diagnosis
     , NVL(parts_used_cte.parts_used,0) AS parts_used
     , COALESCE(lsc.seSpecCond_name, '') AS seStat_name 
     , DATEDIFF(days,se_callTakenUTC,se_workCompletedUTC) AS DaysToResolution
     , COALESCE(efac.locationKey,ifac.locationKey,cfac.locationKey) AS LocationKey
	 , sesc.l_seSpecialConditionID
	 , cpart.qty_ordered AS parts_qty_ordered
     , cpart.qty_received AS parts_qty_received
from unified_db.se_serviceevents sev
	left join purchase_orders_serviceevents_ po on sev.serviceEventID = po.ServiceEventID
		AND sev.DB = po.DB
	left join unified_db.facility_equipment feq on sev.facility_equipmentid = feq.facility_equipmentid
		AND sev.DB = feq.DB
		left join RenovoMaster.l_pmSchedules AS lpms ON feq.l_pmSchedID = lpms.l_pmSchedID
		left join RenovoMaster.mtmodels emtm on feq.mtmodelid = emtm.mtModelID
			left join RenovoMaster.mtdevices emtd on emtm.mtdeviceid = emtd.mtDeviceID
			left join RenovoMaster.mtvendors emtv on emtm.mtvendorid = emtv.mtvendorid
		left join unified_db.mtcldeviceTemps emdt on feq.mtclDeviceTempID = emdt.mtcldevicetempid 
			AND feq.DB = emdt.DB
		left join unified_db.facility_costCenters efcc on feq.facility_costCenterID = efcc.facility_costCenterID
			AND feq.DB = efcc.DB
			left join unified_db.facilities efac on efcc.facilityid = efac.facilityid
				AND efcc.DB = efac.DB
				left join unified_db.clientSystems ecs on efac.systemid = ecs.systemid
					AND efac.DB = ecs.DB
				left join unified_db.clients eccs on ecs.clientid = eccs.clientid
					AND ecs.DB = eccs.DB
	left join unified_db.ismSystems isys on sev.ismsystemid = isys.ismsystemid
		AND sev.DB = isys.DB
		left join RenovoMaster.mtmodels imtm on isys.mtmodelid = imtm.mtModelID
			left join RenovoMaster.mtdevices imtd on imtm.mtdeviceid = imtd.mtDeviceID
			left join RenovoMaster.mtvendors imtv on imtm.mtvendorid = imtv.mtvendorid
		left join unified_db.mtcldeviceTemps imdt on isys.mtclDeviceTempID = imdt.mtcldevicetempid 
			AND isys.DB = imdt.DB
		left join unified_db.facility_costCenters ifcc on isys.facility_costCenterID = ifcc.facility_costCenterID
			AND isys.DB = ifcc.DB
			left join unified_db.facilities ifac on ifcc.facilityid = ifac.facilityid
				AND ifcc.DB = ifac.DB
				left join unified_db.clientSystems ics on ifac.systemid = ics.systemid
					AND ifac.DB = ics.DB
				left join unified_db.clients iccs on ics.clientid = iccs.clientid
					AND ics.DB = iccs.DB
	left join unified_db.facility_costcenters cfcc on sev.facility_costcenterid = cfcc.facility_costcenterid
		AND sev.DB = cfcc.DB
		left join unified_db.facilities cfac on cfcc.facilityID = cfac.facilityID
			AND cfcc.DB = cfac.DB
			left join unified_db.clientSystems ccs on cfac.systemid = ccs.systemid
				AND cfac.DB = ccs.DB
				left join unified_db.clients cccs on ccs.clientid = iccs.clientid
					AND ccs.DB = iccs.DB
	left join unified_db.l_sestatus lss on sev.l_sestatusid = lss.sestatusid
		AND sev.DB = lss.DB
	LEFT JOIN analytics.dim_se_assigned on dim_se_assigned.DB = sev.DB and dim_se_assigned.serviceEventID = sev.serviceEventID
	left join unified_db.se_specialConditions sesc on sev.serviceeventID = sesc.se_serviceeventID
		AND sev.DB = sesc.DB
		left join RenovoMaster.l_lookups ll on sesc.l_seSpecialConditionID = ll.l_lookupID and ll.parentID = 34
    
    LEFT JOIN unified_db.l_seSpecialConditions AS lsc ON sesc.l_seSpecialConditionID = lsc.seSpecCondID
        AND sesc.DB = lsc.DB

    LEFT JOIN analytics.dim_se_notes notes_cte ON sev.serviceEventID = notes_cte.serviceEventID and sev.DB= notes_cte.DB
    LEFT JOIN parts_used_cte on sev.DB = parts_used_cte.DB  AND sev.serviceeventID = parts_used_cte.serviceEventID
	LEFT JOIN unified_db.l_seResolutions AS lrs
	    ON sev.l_seResolutionID = lrs.seResID AND sev.DB = lrs.DB
	LEFT JOIN unified_db.l_seSymptoms AS lsm
	    ON sev.l_seSymptomID = lsm.seSymptID AND sev.DB = lsm.DB
	LEFT JOIN unified_db.l_seDiagnosis AS ldm
	    ON sev.l_seDiagnosisID = ldm.seDiagnID AND sev.DB = ldm.DB
	LEFT JOIN cte_parts_counts as cpart on sev.db = cpart.db and sev.serviceEventID=cpart.serviceEventID
where sev.se_deletedUTC is null
and sev.l_seInitiatedById <> 5
)
select * from final_cte
