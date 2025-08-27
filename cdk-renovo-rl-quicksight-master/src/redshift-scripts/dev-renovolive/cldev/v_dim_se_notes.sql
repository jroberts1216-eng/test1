CREATE OR REPLACE VIEW cldev.v_dim_se_notes AS WITH
serviceeventid_notes2_cte AS (
    SELECT
        sau.serviceEventID,
        LISTAGG(TO_CHAR(TO_TIMESTAMP(note.seNote_created, 'YYYY-MM-DD HH:MI:SS'),'MM/DD/YY HH12:MI:SS AM') || ' by ' || u.user_fullName + ': ' || note.seNote_note, '| ') as note2
	FROM cldev.v_se_assignedUsers sau
    JOIN cldev.v_se_serviceNotes note ON sau.serviceEventID = note.serviceEventID
	JOIN RenovoMaster.v_users u ON note.seNote_createdByUserID = u.userID
	WHERE note.seNote_isSystemNote = 0
    GROUP BY sau.serviceEventID
)
SELECT * FROM serviceeventid_notes2_cte WITH NO SCHEMA BINDING;