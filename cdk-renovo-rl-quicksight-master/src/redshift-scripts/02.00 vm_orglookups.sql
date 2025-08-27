DROP MATERIALIZED VIEW IF EXISTS "analytics"."vm_orglookups";

CREATE MATERIALIZED VIEW "analytics"."vm_orglookups" AS
SELECT
    "k".name AS kind_name,
    COALESCE(e.overrideid, e.l_lookupid) AS l_lookupid,
    e.parentid,
    e.name,
    e.abbrev,
    e.definition,
    e.created,
    e.createduserid,
    e.lastupdated,
    e.lastupdateduserid,
    e.orgid,
    e.overrideid,
    e.l_lookupid AS originalid
FROM (
    (
        SELECT
            lup.l_lookupid,
            lup.parentid,
            lup.name,
            lup.abbrev,
            lup.definition,
            lup.created,
            lup.createduserid,
            lup.lastupdated,
            lup.lastupdateduserid,
            lup.orgid,
            lup.overrideid
        FROM renovomaster.l_lookups lup
        WHERE lup.orgid IS NULL
          AND NOT (
              lup.l_lookupid IN (
                  SELECT o.overrideid
                  FROM (
                      SELECT
                          l_lookups.l_lookupid,
                          l_lookups.parentid,
                          l_lookups.name,
                          l_lookups.abbrev,
                          l_lookups.definition,
                          l_lookups.created,
                          l_lookups.createduserid,
                          l_lookups.lastupdated,
                          l_lookups.lastupdateduserid,
                          l_lookups.orgid,
                          l_lookups.overrideid
                      FROM renovomaster.l_lookups
                      WHERE l_lookups.orgid IS NOT NULL
                        AND l_lookups.overrideid IS NOT NULL
                        AND l_lookups.name::text <> ''::character varying::text
                        AND l_lookups.orgid IN (
                            SELECT organizations.orgid
                            FROM renovomaster.organizations
                        )
                  ) o
                  WHERE o.parentid = lup.parentid
              )
          )
        UNION
        SELECT
            l_lookups.l_lookupid,
            l_lookups.parentid,
            l_lookups.name,
            l_lookups.abbrev,
            l_lookups.definition,
            l_lookups.created,
            l_lookups.createduserid,
            l_lookups.lastupdated,
            l_lookups.lastupdateduserid,
            l_lookups.orgid,
            l_lookups.overrideid
        FROM renovomaster.l_lookups
        WHERE l_lookups.orgid IS NOT NULL
          AND l_lookups.overrideid IS NULL
          AND l_lookups.orgid IN (
              SELECT organizations.orgid
              FROM renovomaster.organizations
          )
    )
    UNION
    SELECT
        overrides.l_lookupid,
        overrides.parentid,
        overrides.name,
        overrides.abbrev,
        overrides.definition,
        overrides.created,
        overrides.createduserid,
        overrides.lastupdated,
        overrides.lastupdateduserid,
        overrides.orgid,
        overrides.overrideid
    FROM (
        SELECT
            l_lookups.l_lookupid,
            l_lookups.parentid,
            l_lookups.name,
            l_lookups.abbrev,
            l_lookups.definition,
            l_lookups.created,
            l_lookups.createduserid,
            l_lookups.lastupdated,
            l_lookups.lastupdateduserid,
            l_lookups.orgid,
            l_lookups.overrideid
        FROM renovomaster.l_lookups
        WHERE l_lookups.orgid IS NOT NULL
          AND l_lookups.overrideid IS NOT NULL
          AND l_lookups.name::text <> ''::character varying::text
          AND l_lookups.orgid IN (
              SELECT organizations.orgid
              FROM renovomaster.organizations
          )
    ) overrides
) e
JOIN renovomaster.l_lookups "k"
    ON e.parentid = "k".l_lookupid
   AND "k".parentid = 0;
