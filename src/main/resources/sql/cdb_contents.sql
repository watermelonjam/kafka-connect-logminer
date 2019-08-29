/**
 * Copyright 2018 David Arnold
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

SELECT
    SCN,
    START_SCN,
    COMMIT_SCN,
    TIMESTAMP,
    TX_NAME,
    OPERATION,
    OPERATION_CODE,
    SEG_OWNER,
    SEG_NAME,
    TABLE_NAME,
    SEG_TYPE_NAME,
    TABLE_SPACE,
    ROW_ID,
    USERNAME,
    SESSION#,
    SERIAL#,
    SESSION_INFO,
    THREAD#,
    SEQUENCE#,
    RBASQN,
    RBABLK,
    SQL_REDO,
    RS_ID,
    CSF,
    INFO,
    STATUS,
    SRC_CON_ID,
    SRC_CON_NAME
FROM
    V$LOGMNR_CONTENTS
WHERE
    OPERATION_CODE IN (
        1,
        2,
        3
    )
    AND  