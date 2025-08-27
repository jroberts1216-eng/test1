DROP MATERIALIZED IF EXISTS view analytics.vi_sm_status_dashboard_detail;

create MATERIALIZED view analytics.vi_sm_status_dashboard_detail
SORTKEY(DB,ServiceEventID)
 AS
with 

po_totals AS (
	SELECT
		db,
		serviceeventid,
		sum(po_total_value) as total
	FROM analytics.dim_purchase_orders
	WHERE po_cancelled is null
	GROUP BY 1,2
 
)

,cte as (
select
	sev.DB,
	cls.clientID::text as client_id,
	cls.systemid::text as system_id,
	c.regionid::text as region_id,
	cls.system_name,
	fac.facilityid as facility_id,
	fac.facility_name,
	fcc.facility_costCenterID,
	fcc.costCenter_name,
	feq.equipment_number AS CETag,
	mMod.mtclModel_name as ModelName,
	mven.mtVendor_name as Manufacturer,
	mDev.mtclDevice_name AS DeviceName,
	feq.Equipment_SerialNumber AS NoSerial,
	case
            when sev.se_sm = '' then NULL
            ELSE to_date(sev.se_sm,'MM - YYYY')
        END AS se_sm,
	CASE
		WHEN usr.se_assigned IS NULL THEN '-- Unassigned --'
		ELSE usr.se_assigned
	END AS technician,
	sev.serviceEventID,
	sev.se_number,
	sev.se_initiatedUTC,
	sev.se_workCompletedUTC,
	sev.se_completedUTC,
	case
		when feq.mtModelID is not null
			then 
				case when mtd.l_riskRatingID is null OR mtd.l_riskRatingID > 1
					then 0
					else 1
				end
			else COALESCE(mTmp.mtclDevTmp_lifeSupport,0)
		end as LifeSupport
	, NVL(sev.se_smComplCode,6) as se_smComplCode
	, po_totals.total
	, case 
		when  sev.se_state = 1 THEN 'Open'
		when  sev.se_state = 2 THEN 'Completed'
		when  sev.se_state = 3 THEN 'Closed'
		ELSE ''
	 END AS se_state_name
	, CASE
		WHEN sc.l_seSpecialConditionID in (2,10) THEN 'Adjusted'
		WHEN sc.l_seSpecialConditionID in (7,8,21) THEN 'Managed'
		Else 'None'
	 END AS ManagedScType
	, CASE
        WHEN sev.se_state = 1 THEN lu.name || ' (' || COALESCE(ls.seStat_name, 'Status Undefined') || ')'
        ELSE lu.name 
	  END AS state_name
    , COALESCE(lsc.seSpecCond_name, '') AS seStat_name 
	, NVL(sev.se_resolution,'') AS resolution
	,sev.l_seInitiatedById
	,feq.l_equipmentStatusID
	,les.equipmentstatus_name as Equipment_Status
from unified_db.se_serviceEvents sev 
join unified_db.facility_equipment feq on sev.facility_equipmentID = feq.facility_equipmentID
	AND sev.DB=feq.DB
LEFT join unified_db.facility_costCenters fcc on feq.facility_costCenterID = fcc.facility_costCenterID
	AND feq.DB=fcc.DB
LEFT join unified_db.facilities fac on fac.facilityID = fcc.facilityID
	AND fac.DB=fcc.DB
LEFT join unified_db.clientSystems cls on cls.systemID = fac.systemID 
	AND cls.DB=fac.DB
LEFT join unified_db.clients c on cls.clientid = c.clientid 
	AND cls.DB=c.DB
left join unified_db.mtclModels mMod on feq.mtModelID = mMod.mtclModelID
	AND feq.DB = mMod.DB
left join unified_db.mtclDevices mDev on mMod.mtclDeviceID = mDev.mtclDeviceID 
	AND mMod.DB = mDev.DB 
left join RenovoMaster.mtDevices mtd on mDev.mtclDeviceID = mtd.mtDeviceID
LEFT JOIN RenovoMaster.mtVendors mVen ON mMod.mtclVendorID = mVen.mtVendorID
left join unified_db.mtclDeviceTemps mTmp on feq.mtclDeviceTempID = mTmp.mtclDeviceTempID 
	AND feq.DB = mTmp.DB 
LEFT JOIN ANALYTICS.dim_se_assigned usr ON feq.db = usr.db
	AND sev.serviceEventID = usr.serviceEventID
LEFT JOIN po_totals ON sev.DB = po_totals.DB AND sev.serviceEventID=po_totals.serviceeventID
LEFT JOIN RenovoMaster.l_lookups AS lu ON sev.se_state = lu.l_lookupID
        AND lu.parentID = 71
LEFT JOIN unified_db.se_specialConditions AS sc ON sev.serviceEventID = sc.se_serviceEventID AND sev.DB = sc.DB
LEFT JOIN unified_db.l_seSpecialConditions AS lsc ON sc.l_seSpecialConditionID = lsc.seSpecCondID
        AND sc.DB = lsc.DB
LEFT JOIN unified_db.l_seStatus AS ls ON sev.l_seStatusID = ls.seStatusID
        AND sev.DB = ls.DB
LEFT JOIN unified_db.l_equipmentStatus AS les ON feq.l_equipmentstatusid = les.l_equipmentstatusid AND feq.DB = les.DB
where sev.se_deletedUTC is null
--AND sev.se_sm <> '' and sev.se_sm is not null
)
select 
*
from cte
