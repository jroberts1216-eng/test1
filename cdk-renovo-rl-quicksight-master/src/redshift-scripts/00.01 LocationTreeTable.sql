create OR REPLACE view analytics.LocationTree AS 
	SELECT cc.db, cc.locationKey as rootLocationKey, cc.locationKey
	FROM unified_db.facility_costCenters cc 
	JOIN unified_db.facilities f ON f.facilityID = cc.facilityID AND f.DB = cc.DB

	UNION

	-- cost center tier with facility root
	SELECT f.DB, f.locationKey, cc.locationKey
	FROM unified_db.facility_costCenters cc 
	JOIN unified_db.facilities f ON f.facilityID = cc.facilityID  AND f.DB = cc.DB

	UNION

	-- cost center tier with system root
	SELECT s.DB, s.locationKey, cc.locationKey
	FROM unified_db.facility_costCenters cc 
	JOIN unified_db.facilities f ON f.facilityID = cc.facilityID AND f.DB = cc.DB
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND s.DB = f.DB

	UNION

	-- cost center tier with client root
	SELECT c.DB, c.locationKey, cc.locationKey
	FROM unified_db.facility_costCenters cc 
	JOIN unified_db.facilities f ON f.facilityID = cc.facilityID AND f.DB = cc.DB
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND s.DB =  f.DB
	JOIN unified_db.clients c ON c.clientID = s.clientID AND c.DB =  s.DB

	UNION

	-- cost center tier with region root
	SELECT r.DB, r.locationKey, cc.locationKey
	FROM unified_db.facility_costCenters cc 
	JOIN unified_db.facilities f ON f.facilityID = cc.facilityID AND f.DB = cc.DB 
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND  s.DB =  f.DB
	JOIN unified_db.clients c ON c.clientID = s.clientID AND  c.DB =  s.DB
	JOIN unified_db.regions r ON r.regionID = c.regionID AND  r.DB =  c.DB

	UNION

	-- cost center tier with organization root
	SELECT f.DB, o.locationKey, cc.locationKey
	FROM unified_db.facility_costCenters cc 
	JOIN unified_db.facilities f ON f.facilityID = cc.facilityID AND f.DB =cc.DB
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND s.DB =f.DB
	JOIN unified_db.clients c ON c.clientID = s.clientID AND c.DB =s.DB
	JOIN unified_db.regions r ON r.regionID = c.regionID AND r.DB =c.DB
	JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

	UNION

	-- facility tier with facility root
	SELECT f.DB, f.locationKey, f.locationKey
	FROM unified_db.facilities f
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND f.DB = s.DB

	UNION

	-- facility tier with system root
	SELECT s.DB, s.locationKey, f.locationKey
	FROM unified_db.facilities f 
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND f.DB = s.DB

	UNION

	-- facility tier with client root
	SELECT c.DB,c.locationKey, f.locationKey
	FROM unified_Db.facilities f 
	JOIN unified_Db.clientSystems s ON s.systemID = f.systemID AND f.DB = s.DB
	JOIN unified_Db.clients c ON c.clientID = s.clientID AND c.DB = s.DB

	UNION

	-- facility tier with region root
	SELECT r.DB,r.locationKey, f.locationKey
	FROM unified_db.facilities f 
	JOIN unified_db.clientSystems s ON s.systemID = f.systemID AND f.DB = s.DB
	JOIN unified_db.clients c ON c.clientID = s.clientID AND c.DB = s.DB
	JOIN unified_db.regions r ON r.regionID = c.regionID AND r.DB = c.DB

	UNION

	-- facility tier with organization root
	SELECT f.DB, o.locationKey, f.locationKey
	FROM unified_Db.facilities f 
	JOIN unified_Db.clientSystems s ON s.systemID = f.systemID AND f.DB = s.DB
	JOIN unified_Db.clients c ON c.clientID = s.clientID AND c.DB = s.DB
	JOIN unified_Db.regions r ON r.regionID = c.regionID AND r.DB = c.DB
	JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

	UNION

	-- system tier with system root
	SELECT s.DB,s.locationKey, s.locationKey
	FROM unified_Db.clientSystems s
	JOIN unified_Db.clients c ON c.clientID = s.clientID AND c.DB = s.DB

	UNION

	-- system tier with client root
	SELECT c.DB, c.locationKey, s.locationKey
	FROM unified_Db.clientSystems s 
	JOIN unified_Db.clients c ON c.clientID = s.clientID AND c.DB = s.DB

	UNION

	-- system tier with region root
	SELECT r.DB, r.locationKey, s.locationKey
	FROM unified_db.clientSystems s 
	JOIN unified_db.clients c ON c.clientID = s.clientID AND c.DB = s.DB 
	JOIN unified_db.regions r ON r.regionID = c.regionID AND r.DB = c.DB

	UNION

	-- system tier with organization root
	SELECT r.DB, o.locationKey, s.locationKey
	FROM unified_Db.clientSystems s 
	JOIN unified_Db.clients c ON c.clientID = s.clientID AND c.DB = s.DB
	JOIN unified_Db.regions r ON r.regionID = c.regionID AND r.DB = c.DB
	JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

	UNION

	-- client tier with client root
	SELECT r.DB, c.locationKey, c.locationKey
	FROM unified_Db.clients c
	JOIN unified_Db.regions r ON r.regionID = c.regionID AND c.DB = r.DB 

	UNION

	-- client tier with region root
	SELECT r.DB, r.locationKey, c.locationKey
	FROM unified_Db.clients c 
	JOIN unified_Db.regions r ON r.regionID = c.regionID AND c.DB = r.DB

	UNION

	-- client tier with organization root
	SELECT r.DB, o.locationKey, c.locationKey
	FROM unified_db.clients c 
	JOIN unified_db.regions r ON r.regionID = c.regionID AND r.DB = c.DB
	JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

	UNION

	-- region tier with region root
	SELECT r.DB,r.locationKey, r.locationKey
	FROM unified_db.regions r
	JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

	UNION

	-- region tier with organization root
	SELECT r.DB, o.locationKey, r.locationKey
	FROM unified_db.regions r 
	JOIN RenovoMaster.organizations o ON o.orgID = r.orgID

	UNION

	-- organization tier with organization root
	SELECT lower(o.org_connectionName) as DB, o.locationKey, o.locationKey
	FROM RenovoMaster.organizations o 
with no schema binding;