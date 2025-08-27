CREATE
OR REPLACE VIEW cldev.v_dim_se_assigned AS
SELECT
    sev1.serviceEventID,
    LISTAGG(u.user_fullName, ', ') WITHIN GROUP (
        ORDER BY
            u.user_fullName ASC
    ) AS se_assigned
FROM
    cldev.v_se_serviceEvents sev1
    JOIN cldev.v_se_assignedUsers seu ON sev1.serviceEventID = seu.serviceEventID
    JOIN RenovoMaster.v_user u ON seu.se_assignedUserID = u.userID
WHERE
    sev1.se_deletedUTC IS NULL
GROUP BY
    sev1.serviceEventID WITH NO SCHEMA BINDING;