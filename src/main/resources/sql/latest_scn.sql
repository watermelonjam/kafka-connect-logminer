SELECT
    MIN(FIRST_CHANGE#) FIRST_CHANGE#
FROM
    (
        SELECT
            FIRST_CHANGE#
        FROM
            V$LOG
        WHERE
            ? BETWEEN FIRST_CHANGE# AND NEXT_CHANGE#
        UNION
        SELECT
            FIRST_CHANGE#
        FROM
            V$ARCHIVED_LOG
        WHERE
            ? BETWEEN FIRST_CHANGE# AND NEXT_CHANGE#
            AND STANDBY_DEST = 'NO'
    )