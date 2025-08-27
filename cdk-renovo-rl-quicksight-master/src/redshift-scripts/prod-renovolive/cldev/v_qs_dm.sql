CREATE OR REPLACE VIEW cldev.v_qs_dm AS
WITH filtered_se_data AS (
	SELECT * FROM cldev.v_se_serviceevents
	WHERE se_deletedUTC IS NULL
	AND l_seInitiatedById <> 5
)
SELECT
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN ecs.systemid
        WHEN sev.ismsystemid IS NOT NULL THEN ics.systemid
        ELSE ccs.systemid
    END AS system_id,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN efac.facilityid
        WHEN sev.ismsystemid IS NOT NULL THEN ifac.facilityid
        ELSE cfac.facilityid
    END AS facility_id,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN ecs.clientID
        WHEN sev.ismsystemid IS NOT NULL THEN ics.clientID
        ELSE ccs.clientID
    END AS client_id,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN eccs.regionID
        WHEN sev.ismsystemid IS NOT NULL THEN iccs.regionID
        ELSE cccs.regionID
    END AS region_id,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN ecs.system_name
        WHEN sev.ismsystemid IS NOT NULL THEN ics.system_name
        ELSE ccs.system_name
    END AS system_name,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN efac.facility_name
        WHEN sev.ismsystemid IS NOT NULL THEN ifac.facility_name
        ELSE cfac.facility_name
    END AS facility,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN efcc.costcenter_name
        WHEN sev.ismsystemid IS NOT NULL THEN ifcc.costCenter_name
        ELSE cfcc.costcenter_name
    END AS costcenter,
    sev.se_callTakenUTC AS ReceivedDate,
    sev.se_workCompletedUTC AS se_workCompletedUTC,
    sev.serviceeventID AS serviceeventID,
    sev.se_number AS se_no,
    CASE
        WHEN sev.se_state = 3 THEN 'Closed'
        WHEN sev.se_state = 2 THEN 'Completed'
        ELSE lss.seStat_name
    END AS status,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN feq.equipment_number
        WHEN sev.ismsystemid IS NOT NULL THEN isys.system_siteID
        ELSE 'CC: ' || cfcc.costcenter_name
    END AS ceTagNo,
    feq.equipment_SerialNumber AS SerialNumber,
    feq.equipment_location AS "Location",
    feq.equipment_altNumber AS FacAssetNumber,
    feq.IsMissionCritical AS IsMissionCritical,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN 
            CASE
                WHEN feq.mtmodelid IS NOT NULL THEN emtd.mtDevice_name
                ELSE emdt.mtclDevTmp_device
            END
        WHEN sev.ismsystemid IS NOT NULL THEN
            CASE
                WHEN isys.mtmodelid IS NOT NULL THEN imtd.mtdevice_name
                ELSE imdt.mtcldevtmp_device
            END
        ELSE ''
    END AS Device,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN 
            CASE
                WHEN feq.mtModelID IS NOT NULL THEN emtv.mtvendor_name
                ELSE emdt.mtclDevTmp_vendor
            END
        WHEN sev.ismsystemid IS NOT NULL THEN
            CASE
                WHEN isys.mtmodelid IS NOT NULL THEN imtv.mtvendor_name
                ELSE imdt.mtcldevTmp_vendor
            END
        ELSE ''
    END AS Manufacturer,
    CASE
        WHEN sev.facility_equipmentid IS NOT NULL THEN 
            CASE
                WHEN feq.mtmodelid IS NOT NULL THEN emtm.mtmodel_name
                ELSE emdt.mtclDevTmp_model
            END
        WHEN sev.ismsystemid IS NOT NULL THEN
            CASE
                WHEN isys.mtModelID IS NOT NULL THEN imtm.mtmodel_name
                ELSE imdt.mtcldevtmp_model
            END
        ELSE ''
    END AS "Model",
    CASE
        WHEN sev.l_seInitiatedById = 5 THEN 'SM'
        ELSE 'DM'
    END AS SMDM,
    (COALESCE(sev.l_seSymptomID, 0) = 7) AS Is_Incoming_Inspection,
    lpms.pmSched_name AS SmSched,
    COALESCE(dim_se_assigned.se_assigned, '-- Unassigned --') AS Technician,
    po.po_number AS PO_Number,
    ll.name AS specialCondition,
    CASE
        WHEN sev.se_completedUTC IS NULL THEN 'Open'
        ELSE 'Closed'
    END AS se_status,
    sev.se_completedUTC AS se_completedUTC,
    sev.se_sm::text AS se__sm,
    sev.se_notes AS Notes1,
    notes_cte.note2 AS Notes2,
    CASE
        WHEN sev.l_sePriorityID = 1 THEN 'Emergency Down'
        WHEN sev.l_sePriorityID = 2 THEN 'Urgent'
        WHEN sev.l_sePriorityID = 3 THEN 'Routine'
        ELSE ''
    END AS Priority,
    TRIM(sev.se_problem) AS problem,
    TRIM(sev.se_problemtext) AS problem_text,
    COALESCE(TRIM(sev.se_resolution), '') AS resolution,
    COALESCE(lrs.seRes_name, '') AS resolutionCode,
    COALESCE(TRIM(lsm.seSympt_name), '') AS symptom,
    COALESCE(TRIM(ldm.seDiagn_name), '') AS diagnosis,
    COALESCE(cldev.v_dim_total_parts_used_by_se.parts_used, 0) AS parts_used,
    COALESCE(lsc.seSpecCond_name, '') AS seStat_name,
    DATEDIFF(days, se_callTakenUTC, se_workCompletedUTC) AS DaysToResolution,
    COALESCE(efac.locationKey, ifac.locationKey, cfac.locationKey) AS LocationKey,
    sesc.l_seSpecialConditionID AS l_seSpecialConditionID,
    cpart.qty_ordered AS parts_qty_ordered,
    cpart.qty_received AS parts_qty_received
    sev.se_lastUpdatedUTC
FROM filtered_se_data AS sev
LEFT JOIN cldev.v_dim_po_numbers_by_se AS po
    ON sev.serviceEventID = po.ServiceEventID
LEFT JOIN cldev.v_facility_equipment AS feq
    ON sev.facility_equipmentid = feq.facility_equipmentid
LEFT JOIN RenovoMaster.v_l_pmSchedules AS lpms
    ON feq.l_pmSchedID = lpms.l_pmSchedID
LEFT JOIN RenovoMaster.v_mtmodels AS emtm
    ON feq.mtmodelid = emtm.mtModelID
LEFT JOIN RenovoMaster.v_mtdevices AS emtd
    ON emtm.mtdeviceid = emtd.mtDeviceID
LEFT JOIN RenovoMaster.v_mtvendors AS emtv
    ON emtm.mtvendorid = emtv.mtvendorid
LEFT JOIN cldev.v_mtcldeviceTemps AS emdt
    ON feq.mtclDeviceTempID = emdt.mtcldevicetempid
LEFT JOIN cldev.v_facility_costCenters AS efcc
    ON feq.facility_costCenterID = efcc.facility_costCenterID
LEFT JOIN cldev.v_facilities AS efac
    ON efcc.facilityid = efac.facilityid
LEFT JOIN cldev.v_clientSystems AS ecs
    ON efac.systemid = ecs.systemid
LEFT JOIN cldev.v_clients AS eccs
    ON ecs.clientid = eccs.clientid
LEFT JOIN cldev.v_ismSystems AS isys
    ON sev.ismsystemid = isys.ismsystemid
LEFT JOIN RenovoMaster.v_mtmodels AS imtm
    ON isys.mtmodelid = imtm.mtModelID
LEFT JOIN RenovoMaster.v_mtdevices AS imtd
    ON imtm.mtdeviceid = imtd.mtDeviceID
LEFT JOIN RenovoMaster.v_mtvendors AS imtv
    ON imtm.mtvendorid = imtv.mtvendorid
LEFT JOIN cldev.v_mtcldeviceTemps AS imdt
    ON isys.mtclDeviceTempID = imdt.mtcldevicetempid
LEFT JOIN cldev.v_facility_costCenters AS ifcc
    ON isys.facility_costCenterID = ifcc.facility_costCenterID
LEFT JOIN cldev.v_facilities AS ifac
    ON ifcc.facilityid = ifac.facilityid
LEFT JOIN cldev.v_clientSystems AS ics
    ON ifac.systemid = ics.systemid
LEFT JOIN cldev.v_clients AS iccs
    ON ics.clientid = iccs.clientid
LEFT JOIN cldev.v_facility_costcenters AS cfcc
    ON sev.facility_costcenterid = cfcc.facility_costcenterid
LEFT JOIN cldev.v_facilities AS cfac
    ON cfcc.facilityID = cfac.facilityID
LEFT JOIN cldev.v_clientSystems AS ccs
    ON cfac.systemid = ccs.systemid
LEFT JOIN cldev.v_clients AS cccs
    ON ccs.clientid = cccs.clientid
LEFT JOIN cldev.v_l_sestatus AS lss
    ON sev.l_sestatusid = lss.sestatusid
LEFT JOIN cldev.v_dim_se_assigned AS dim_se_assigned
    ON dim_se_assigned.serviceEventID = sev.serviceEventID
LEFT JOIN cldev.v_se_specialConditions AS sesc
    ON sev.serviceEventID = sesc.se_serviceeventID
LEFT JOIN RenovoMaster.v_l_lookups AS ll
    ON sesc.l_seSpecialConditionID = ll.l_lookupID
    AND ll.parentID = 34
LEFT JOIN cldev.v_l_seSpecialConditions AS lsc
    ON sesc.l_seSpecialConditionID = lsc.seSpecCondID
LEFT JOIN cldev.v_dim_se_notes AS notes_cte
    ON sev.serviceEventID = notes_cte.serviceEventID
LEFT JOIN cldev.v_dim_total_parts_used_by_se
    ON sev.serviceEventID = cldev.v_dim_total_parts_used_by_se.serviceEventID
LEFT JOIN cldev.v_l_seResolutions AS lrs
    ON sev.l_seResolutionID = lrs.seResID
LEFT JOIN cldev.v_l_seSymptoms AS lsm
    ON sev.l_seSymptomID = lsm.seSymptID
LEFT JOIN cldev.v_l_seDiagnosis AS ldm
    ON sev.l_seDiagnosisID = ldm.seDiagnID
LEFT JOIN cldev.v_dim_total_parts_ordered_by_se AS cpart
    ON sev.serviceEventID = cpart.serviceEventID
WHERE sev.se_deletedUTC IS NULL
    AND sev.l_seInitiatedById <> 5
WITH NO SCHEMA BINDING;
