
CREATE OR REPLACE VIEW analytics.Locations
AS
SELECT cc.DB,cc.locationKey, cc.facility_costCenterID as locationID, cc.costCenter_name as locationName , -1 as locationType, cc.isInactive, f.locationKey as parentLocationKey
FROM unified_Db.facility_costCenters cc
JOIN unified_Db.facilities f ON f.facilityID = cc.facilityID AND f.DB = cc.DB

UNION

SELECT f.DB, f.locationKey, f.facilityID, f.facility_name, 0, f.isInactive, s.locationKey
FROM unified_Db.facilities f
JOIN unified_Db.clientSystems s ON s.systemID = f.systemID AND s.DB = f.DB

UNION
    
SELECT s.DB, s.locationKey, s.systemID, s.system_name, 1, s.isInactive, c.locationKey
FROM unified_db.clientSystems s 
JOIN unified_db.clients c ON c.clientID = s.clientID AND s.DB = c.DB

UNION

SELECT c.DB, c.locationKey, c.clientID, c.client_name, 2, c.isInactive, r.locationKey
FROM unified_db.clients c
JOIN unified_db.regions r ON r.regionID = c.regionID AND c.DB = r.DB

UNION

SELECT r.DB, r.locationKey, r.regionID, r.region_name, 3, r.isInactive, o.locationKey
FROM unified_Db.regions r
JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

UNION

SELECT lower(org_connectionName) as DB ,locationKey, orgID, org_name, 4, 0::boolean, NULL
FROM RenovoMaster.organizations
with no schema binding;

