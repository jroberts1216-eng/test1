CREATE TABLE unifieddb.locationtree_green AS
	SELECT cc.db, cc.locationKey AS rootLocationKey, cc.locationKey
	FROM unifieddb.v_facility_costCenters_spectrum AS cc
	JOIN unifieddb.v_facilities_spectrum AS f ON f.facilityID = cc.facilityID AND f.db = cc.db

	UNION ALL

	-- Cost center tier with facility root
	SELECT f.db, f.locationKey AS rootLocationKey, cc.locationKey
	FROM unifieddb.v_facility_costCenters_spectrum AS cc
	JOIN unifieddb.v_facilities_spectrum AS f ON f.facilityID = cc.facilityID AND f.db = cc.db

	UNION ALL

	-- Cost center tier with system root
	SELECT s.db, s.locationKey AS rootLocationKey, cc.locationKey
	FROM unifieddb.v_facility_costCenters_spectrum AS cc
	JOIN unifieddb.v_facilities_spectrum AS f ON f.facilityID = cc.facilityID AND f.db = cc.db
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND s.db = f.db

	UNION ALL

	-- Cost center tier with client root
	SELECT c.db, c.locationKey AS rootLocationKey, cc.locationKey
	FROM unifieddb.v_facility_costCenters_spectrum AS cc
	JOIN unifieddb.v_facilities_spectrum AS f ON f.facilityID = cc.facilityID AND f.db = cc.db
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND s.db = f.db
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db

	UNION ALL

	-- Cost center tier with region root
	SELECT r.db, r.locationKey AS rootLocationKey, cc.locationKey
	FROM unifieddb.v_facility_costCenters_spectrum AS cc
	JOIN unifieddb.v_facilities_spectrum AS f ON f.facilityID = cc.facilityID AND f.db = cc.db
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND s.db = f.db
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db

	UNION ALL

	-- Cost center tier with organization root
	SELECT f.db, o.locationKey AS rootLocationKey, cc.locationKey
	FROM unifieddb.v_facility_costCenters_spectrum AS cc
	JOIN unifieddb.v_facilities_spectrum AS f ON f.facilityID = cc.facilityID AND f.db = cc.db
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND s.db = f.db
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db
	JOIN RenovoMaster.v_organizations_spectrum AS o ON o.orgID = r.orgID

	UNION ALL

	-- Facility tier with facility root
	SELECT f.db, f.locationKey AS rootLocationKey, f.locationKey
	FROM unifieddb.v_facilities_spectrum AS f
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND f.db = s.db

	UNION ALL

	-- Facility tier with system root
	SELECT s.db, s.locationKey AS rootLocationKey, f.locationKey
	FROM unifieddb.v_facilities_spectrum AS f
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND f.db = s.db

	UNION ALL

	-- Facility tier with client root
	SELECT c.db, c.locationKey AS rootLocationKey, f.locationKey
	FROM unifieddb.v_facilities_spectrum AS f
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND f.db = s.db
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db

	UNION ALL

	-- Facility tier with region root
	SELECT r.db, r.locationKey AS rootLocationKey, f.locationKey
	FROM unifieddb.v_facilities_spectrum AS f
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND f.db = s.db
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db

	UNION ALL

	-- Facility tier with organization root
	SELECT f.db, o.locationKey AS rootLocationKey, f.locationKey
	FROM unifieddb.v_facilities_spectrum AS f
	JOIN unifieddb.v_clientSystems_spectrum AS s ON s.systemID = f.systemID AND f.db = s.db
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db
	JOIN RenovoMaster.v_organizations_spectrum AS o ON o.orgID = r.orgID

	UNION ALL

	-- System tier with system root
	SELECT s.db, s.locationKey AS rootLocationKey, s.locationKey
	FROM unifieddb.v_clientSystems_spectrum AS s
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db

	UNION ALL

	-- System tier with client root
	SELECT c.db, c.locationKey AS rootLocationKey, s.locationKey
	FROM unifieddb.v_clientSystems_spectrum AS s
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db

	UNION ALL

	-- System tier with region root
	SELECT r.db, r.locationKey AS rootLocationKey, s.locationKey
	FROM unifieddb.v_clientSystems_spectrum AS s
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db

	UNION ALL

	-- System tier with organization root
	SELECT r.db, o.locationKey AS rootLocationKey, s.locationKey
	FROM unifieddb.v_clientSystems_spectrum AS s
	JOIN unifieddb.v_clients_spectrum AS c ON c.clientID = s.clientID AND c.db = s.db
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db
	JOIN RenovoMaster.v_organizations_spectrum AS o ON o.orgID = r.orgID

	UNION ALL

	-- Client tier with client root
	SELECT r.db, c.locationKey AS rootLocationKey, c.locationKey
	FROM unifieddb.v_clients_spectrum AS c
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND c.db = r.db

	UNION ALL

	-- Client tier with region root
	SELECT r.db, r.locationKey AS rootLocationKey, c.locationKey
	FROM unifieddb.v_clients_spectrum AS c
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND c.db = r.db

	UNION ALL

	-- Client tier with organization root
	SELECT r.db, o.locationKey AS rootLocationKey, c.locationKey
	FROM unifieddb.v_clients_spectrum AS c
	JOIN unifieddb.v_regions_spectrum AS r ON r.regionID = c.regionID AND r.db = c.db
	JOIN RenovoMaster.v_organizations_spectrum AS o ON o.orgID = r.orgID

	UNION ALL

	-- Region tier with region root
	SELECT r.db, r.locationKey AS rootLocationKey, r.locationKey
	FROM unifieddb.v_regions_spectrum AS r
	JOIN RenovoMaster.v_organizations_spectrum AS o ON o.orgID = r.orgID

	UNION ALL

	-- Region tier with organization root
	SELECT r.db, o.locationKey AS rootLocationKey, r.locationKey
	FROM unifieddb.v_regions_spectrum AS r
	JOIN RenovoMaster.v_organizations_spectrum AS o ON o.orgID = r.orgID

	UNION ALL

	-- Organization tier with organization root
	SELECT LOWER(o.org_connectionName) AS db, o.locationKey AS rootLocationKey, o.locationKey
	FROM RenovoMaster.v_organizations_spectrum AS o
