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

SELECT C.NAME,
T.OWNER,
T.TABLE_NAME
FROM CDB_TABLES T
JOIN (
  SELECT 1 AS CON_ID, DBID, NAME FROM V$DATABASE
  UNION
  SELECT CON_ID, DBID, NAME FROM V$PDBS
) C ON T.CON_ID = C.CON_ID