DROP MATERIALIZED VIEW  IF EXISTS analytics.vFacilityManagementRoles;
create MATERIALIZED view analytics.vFacilityManagementRoles 
BACKUP NO 
DISTSTYLE ALL AS 
WITH locationRoleUsers AS (
    SELECT
        DB,
        locationKey,
        userID,
        locationRoleID,
    NULL AS
        poApprovalAmount
    FROM
        unified_db.locationrole_users
    UNION
    SELECT
        DB,
        locationKey,
        defaultInvoiceAssigneeUserId AS userID,
        -1 AS locationRoleID,
    NULL AS
        poApprovalAmount
    FROM
        unified_db.facilities
    WHERE
        COALESCE(defaultInvoiceAssigneeUserId:: int, 0) <> 0
    UNION
    SELECT
        DB,
        locationKey,
        userID,
        -2 AS locationRoleID,
        settingValue AS poApprovalAmount
    FROM
        unified_db.localPermission_userSettings
    WHERE
        localPermissionID = 41 -- ApprovePurchaseOrders
)
SELECT
    f.DB,
    f.facilityID,
    u.userID,
    MAX(
        CASE
        WHEN u.locationRoleID:: int = 1 THEN 1
        ELSE 0 END
    ) AS isPrimaryManager,
    MAX(
        CASE
        WHEN u.locationRoleID:: int = 2 THEN 1
        ELSE 0 END
    ) AS facilityManager,
    MAX(
        CASE
        WHEN u.locationRoleID:: int = 3 THEN 1
        ELSE 0 END
    ) AS budgetInternalApproval,
    MAX(
        CASE
        WHEN u.locationRoleID:: int = -1 THEN 1
        ELSE 0 END
    ) AS isAPManager,
    MAX(COALESCE(u.poApprovalAmount:: float, 0.0)) AS poApprovalAmount
FROM
    unified_db.facilities f
    JOIN locationRoleUsers u ON f.locationKey = u.locationKey
    AND f.DB = u.DB
GROUP BY
    f.DB,
    f.facilityID,
    u.userID;