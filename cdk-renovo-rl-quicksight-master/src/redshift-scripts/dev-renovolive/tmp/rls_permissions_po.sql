CREATE TABLE unifieddb.rls_permissions_po_green AS
    WITH LocationDetails AS
    (
        SELECT
            db,
            locationKey,
            CASE WHEN locationType = 0 THEN locationID ELSE NULL END AS facilityID,
            CASE WHEN locationType = 1 THEN locationID ELSE NULL END AS systemID,
            CASE WHEN locationType = 2 THEN locationID ELSE NULL END AS clientID,
            CASE WHEN locationType = 3 THEN locationID ELSE NULL END AS regionID,
            CASE WHEN locationType = 4 THEN locationID ELSE NULL END AS orgID
        FROM 
            unifieddb.v_locations
    ),
    GlobalPermissions AS 
    (
        SELECT DISTINCT
            ur.db,
            ur.userID
        FROM
            unifieddb.v_cluser_roles_spectrum ur 
        INNER JOIN 
            unifieddb.v_clRole_permissions_spectrum rp ON rp.roleID = ur.roleID
        WHERE
            rp.permissionID = 115 -- View Purchase Orders
    ),
    LocalPermissions AS
    (
        SELECT DISTINCT
            lru.db,
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
            GlobalPermissions AS gp ON gp.userID = lru.userID
        INNER JOIN 
            unifieddb.v_locationRole_permissions_spectrum lrp ON lrp.locationRoleID = lru.locationRoleID
        INNER JOIN 
            renovomaster.v_users_spectrum u ON lru.userID = u.userID
        LEFT JOIN 
            LocationDetails ld ON lru.locationKey = ld.locationKey
        WHERE 
            lrp.localPermissionID = 99 -- Area Roles

        UNION 

        SELECT
            gp.db,
            gp.userID,
            u.email AS userName,
            0 AS facilityID,
            0 AS systemID,
            0 AS clientID,
            0 AS regionID,
            NULL AS orgID
        FROM
            GlobalPermissions gp
        INNER JOIN 
            renovomaster.v_users_spectrum u ON gp.userID = u.userID
    ),
    AggregatedPermissions AS
    (
        SELECT
            lp.db,
            lp.userID,
            lp.UserName,
            LISTAGG(lp.facilityID::VARCHAR, ',') AS facilityIDs,
            LISTAGG(lp.systemID::VARCHAR, ',') AS systemIDs,
            LISTAGG(lp.clientID::VARCHAR, ',') AS clientIDs,
            LISTAGG(lp.regionID::VARCHAR, ',') AS regionIDs,
            LISTAGG(lp.orgID::VARCHAR, ',') AS orgIDs
        FROM 
            LocalPermissions lp
        GROUP BY
            lp.db,
            lp.userID, 
            lp.UserName
    )
    SELECT
        db,
        userID,
        userName,
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
        db,
        userID,
        userName,
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
        db,
        userID,
        userName,
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
        db,
        userID,
        userName,
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
        db,
        userID,
        userName,
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

