

create OR REPLACE view analytics.RLS_PERMISSIONS aS (


WITH LocationDetails AS
(
    SELECT 
        DB,
        locationKey,
        CASE WHEN locationType = 0 THEN locationID ELSE NULL END AS facilityID,
        CASE WHEN locationType = 1 THEN locationID ELSE NULL END AS systemID,
        CASE WHEN locationType = 2 THEN locationID ELSE NULL END AS clientID,
        CASE WHEN locationType = 3 THEN locationID ELSE NULL END AS regionID,
        CASE WHEN locationType = 4 THEN locationID ELSE NULL END AS orgID
    FROM 
        analytics.locations
)
, UserPermissions AS (
    SELECT DISTINCT
        lru.DB,
        lru.userID,
        u.email AS userName,
        ld.facilityID,
        ld.systemID,
        ld.clientID,
        ld.regionID,
        ld.orgID
    FROM 
        unified_db.locationRole_users lru
    INNER JOIN 
        unified_db.locationRole_permissions lrp ON lrp.locationRoleID = lru.locationRoleID AND lru.DB = lrp.DB
    INNER JOIN 
        renovomaster.users u ON lru.userID = u.userID
    LEFT JOIN 
        LocationDetails ld ON lru.locationKey = ld.locationKey AND lru.DB = ld.DB
    WHERE 
        lrp.localPermissionID = 99 and u.email <>''
)
, AggregatedPermissions AS (
    SELECT
        up.DB,
        up.userName
    FROM 
        UserPermissions up
    WHERE up.userName <> ''
    GROUP BY 
        up.DB,
        up.userName
)
SELECT
    COALESCE(ap.userName,v.username) as userName,
    ap.DB AS DB,
    v.facilityIDs as facility_id,
    v.systemIDs as system_id,
    v.clientIDs as client_id,
    v.regionIDs as region_id
FROM
    AggregatedPermissions ap
-- Replace CROSS APPLY with UNION ALL
RIGHT JOIN
(
    SELECT up.userName, up.DB, LISTAGG(distinct up.facilityID,',') AS facilityIDs, NULL AS systemIDs, NULL AS clientIDs, NULL AS regionIDs, NULL AS orgIDs
    FROM UserPermissions up WHERE up.facilityID IS NOT NULL
    GROUP BY up.userName,up.DB

    UNION ALL

    SELECT up.userName, up.DB, null AS facilityIDs, LISTAGG(distinct up.systemid,',') AS systemIDs, NULL AS clientIDs, NULL AS regionIDs, NULL AS orgIDs
    FROM UserPermissions up WHERE up.systemid IS NOT NULL
    GROUP BY up.userName,up.DB

    UNION ALL

    SELECT up.userName, up.DB, null AS facilityIDs,NULL AS systemIDs,  LISTAGG(distinct up.clientid,',') AS clientIDs, NULL AS regionIDs, NULL AS orgIDs
    FROM UserPermissions up WHERE up.clientid IS NOT NULL
    GROUP BY up.userName,up.DB

    UNION ALL

    SELECT up.userName, up.DB, null AS facilityIDs, NULL AS systemIDs, NULL AS clientIDs, LISTAGG(distinct up.regionID,',')  AS regionIDs, NULL AS orgIDs
    FROM UserPermissions up WHERE up.regionid IS NOT NULL
    GROUP BY up.userName,up.DB

    UNION ALL

    SELECT up.userName, up.DB, null AS facilityIDs, null AS systemIDs, NULL AS clientIDs, NULL AS regionIDs, LISTAGG(distinct up.orgid,',')  AS orgIDs
    FROM UserPermissions up WHERE up.orgid IS NOT NULL
    GROUP BY up.userName,up.DB
) v ON ap.userName = v.userName AND ap.DB = v.DB
ORDER BY
    ap.userName)
with no schema binding
;