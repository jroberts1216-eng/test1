DROP MATERIALIZED VIEW  IF EXISTS ANALYTICS.dim_se_notes;

CREATE MATERIALIZED VIEW ANALYTICS.dim_se_notes 
BACKUP NO 
SORTKEY(DB,ServiceEventID)
AS

WITH
 serviceeventid_notes2_cte as (
    SELECT
	    sau.DB,
        sau.serviceEventID,
        LISTAGG(TO_CHAR(TO_TIMESTAMP(note.seNote_created, 'YYYY-MM-DD HH:MI:SS'),'MM/DD/YY HH12:MI:SS AM') || ' by ' || u.user_fullName + ': ' || note.seNote_note, '| ') as note2
	FROM unified_db.se_assignedUsers sau
    JOIN unified_db.se_serviceNotes note ON sau.serviceEventID = note.serviceEventID AND sau.DB = note.DB
	JOIN RenovoMAster.users u ON note.seNote_createdByUserID = u.userID
	WHERE note.seNote_isSystemNote = 0
    GROUP BY 1,2
)
SELECT * FROM serviceeventid_notes2_cte