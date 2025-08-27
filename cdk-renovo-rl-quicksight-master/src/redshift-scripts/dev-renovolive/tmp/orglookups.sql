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
        FROM renovomaster.v_l_lookups_spectrum lup
        WHERE lup.orgid IS NULL
          AND NOT (
              lup.l_lookupid IN (
                  SELECT o.overrideid
                  FROM (
                      SELECT
                          v_l_lookups_spectrum.l_lookupid,
                          v_l_lookups_spectrum.parentid,
                          v_l_lookups_spectrum.name,
                          v_l_lookups_spectrum.abbrev,
                          v_l_lookups_spectrum.definition,
                          v_l_lookups_spectrum.created,
                          v_l_lookups_spectrum.createduserid,
                          v_l_lookups_spectrum.lastupdated,
                          v_l_lookups_spectrum.lastupdateduserid,
                          v_l_lookups_spectrum.orgid,
                          v_l_lookups_spectrum.overrideid
                      FROM renovomaster.v_l_lookups_spectrum
                      WHERE v_l_lookups_spectrum.orgid IS NOT NULL
                        AND v_l_lookups_spectrum.overrideid IS NOT NULL
                        AND v_l_lookups_spectrum.name::text <> ''::character varying::text
                        AND v_l_lookups_spectrum.orgid IN (
                            SELECT v_organizations_spectrum.orgid
                            FROM renovomaster.v_organizations_spectrum
                        )
                  ) o
                  WHERE o.parentid = lup.parentid
              )
          )
        UNION
        SELECT
            v_l_lookups_spectrum.l_lookupid,
            v_l_lookups_spectrum.parentid,
            v_l_lookups_spectrum.name,
            v_l_lookups_spectrum.abbrev,
            v_l_lookups_spectrum.definition,
            v_l_lookups_spectrum.created,
            v_l_lookups_spectrum.createduserid,
            v_l_lookups_spectrum.lastupdated,
            v_l_lookups_spectrum.lastupdateduserid,
            v_l_lookups_spectrum.orgid,
            v_l_lookups_spectrum.overrideid
        FROM renovomaster.v_l_lookups_spectrum
        WHERE v_l_lookups_spectrum.orgid IS NOT NULL
          AND v_l_lookups_spectrum.overrideid IS NULL
          AND v_l_lookups_spectrum.orgid IN (
              SELECT v_organizations_spectrum.orgid
              FROM renovomaster.v_organizations_spectrum
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
            v_l_lookups_spectrum.l_lookupid,
            v_l_lookups_spectrum.parentid,
            v_l_lookups_spectrum.name,
            v_l_lookups_spectrum.abbrev,
            v_l_lookups_spectrum.definition,
            v_l_lookups_spectrum.created,
            v_l_lookups_spectrum.createduserid,
            v_l_lookups_spectrum.lastupdated,
            v_l_lookups_spectrum.lastupdateduserid,
            v_l_lookups_spectrum.orgid,
            v_l_lookups_spectrum.overrideid
        FROM renovomaster.v_l_lookups_spectrum
        WHERE v_l_lookups_spectrum.orgid IS NOT NULL
          AND v_l_lookups_spectrum.overrideid IS NOT NULL
          AND v_l_lookups_spectrum.name::text <> ''::character varying::text
          AND v_l_lookups_spectrum.orgid IN (
              SELECT v_organizations_spectrum.orgid
              FROM renovomaster.v_organizations_spectrum
          )
    ) overrides
) e
JOIN renovomaster.v_l_lookups_spectrum "k"
    ON e.parentid = "k".l_lookupid
   AND "k".parentid = 0;