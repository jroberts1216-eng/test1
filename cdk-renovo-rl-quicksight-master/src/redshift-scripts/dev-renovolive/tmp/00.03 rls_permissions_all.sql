WITH LocationDetails AS
(
    SELECT 
        locationKey,
        CASE WHEN locationType = 0 THEN locationID ELSE NULL END AS facilityID,
        CASE WHEN locationType = 1 THEN locationID ELSE NULL END AS systemID,
        CASE WHEN locationType = 2 THEN locationID ELSE NULL END AS clientID,
        CASE WHEN locationType = 3 THEN locationID ELSE NULL END AS regionID,
        CASE WHEN locationType = 4 THEN locationID ELSE NULL END AS orgID
    FROM 
        unifieddb.v_locations
),
UserPermissions AS
(
    SELECT DISTINCT
        lru.userID,
        u.email AS userName,
        ld.facilityID,
        ld.systemID,
        ld.clientID,
        ld.regionID,
        ld.orgID
    FROM 
        unifieddb.v_locationRole_users_spectrum lru
    INNER JOIN 
        unifieddb.v_locationRole_permissions_spectrum lrp ON lrp.locationRoleID = lru.locationRoleID
    INNER JOIN 
        renovomaster.v_users_spectrum u ON lru.userID = u.userID
    LEFT JOIN 
        LocationDetails ld ON lru.locationKey = ld.locationKey
    -- WHERE 
    --     lrp.localPermissionID = 99
),
AggregatedPermissions AS
(
    SELECT 
        up.userID,
        up.UserName,
        LISTAGG(up.facilityID::VARCHAR, ',') AS facilityIDs,
        LISTAGG(up.systemID::VARCHAR, ',') AS systemIDs,
        LISTAGG(up.clientID::VARCHAR, ',') AS clientIDs,
        LISTAGG(up.regionID::VARCHAR, ',') AS regionIDs,
        LISTAGG(up.orgID::VARCHAR, ',') AS orgIDs
    FROM 
        UserPermissions up
    GROUP BY 
        up.userID, 
        up.UserName
)
SELECT
    userID,
    userName,
    'sourceDb' AS sourceDb,
    facilityIDs,
    NULL AS systemIDs,
    NULL AS clientIDs,
    NULL AS regionIDs,
    NULL AS orgIDs
FROM 
    AggregatedPermissions
WHERE facilityIDs IS NOT NULL

UNION ALL

SELECT 
    userID,
    userName,
    'sourceDb' AS sourceDb,
    NULL AS facilityIDs,
    systemIDs,
    NULL AS clientIDs,
    NULL AS regionIDs,
    NULL AS orgIDs
FROM 
    AggregatedPermissions
WHERE systemIDs IS NOT NULL

UNION ALL

SELECT 
    userID,
    userName,
    'sourceDb' AS sourceDb,
    NULL AS facilityIDs,
    NULL AS systemIDs,
    clientIDs,
    NULL AS regionIDs,
    NULL AS orgIDs
FROM 
    AggregatedPermissions
WHERE clientIDs IS NOT NULL

UNION ALL

SELECT 
    userID,
    userName,
    'sourceDb' AS sourceDb,
    NULL AS facilityIDs,
    NULL AS systemIDs,
    NULL AS clientIDs,
    regionIDs,
    NULL AS orgIDs
FROM 
    AggregatedPermissions
WHERE regionIDs IS NOT NULL

UNION ALL

SELECT 
    userID,
    userName,
    'sourceDb' AS sourceDb,
    NULL AS facilityIDs,
    NULL AS systemIDs,
    NULL AS clientIDs,
    NULL AS regionIDs,
    orgIDs
FROM 
    AggregatedPermissions
WHERE orgIDs IS NOT NULL
ORDER BY
    userID,
    userName;
