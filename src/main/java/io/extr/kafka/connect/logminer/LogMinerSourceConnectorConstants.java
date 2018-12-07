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

package io.extr.kafka.connect.logminer;

public class LogMinerSourceConnectorConstants {
	public static final String FIELD_SCN = "SCN";
	public static final String FIELD_START_SCN = "START_SCN";
	public static final String FIELD_COMMIT_SCN = "COMMIT_SCN";
	public static final String FIELD_TIMESTAMP = "TIMESTAMP";
	public static final String FIELD_TX_NAME = "TX_NAME";
	public static final String FIELD_OPERATION = "OPERATION";
	public static final String FIELD_OPERATION_CODE = "OPERATION_CODE";
	public static final String FIELD_SEG_OWNER = "SEG_OWNER";
	public static final String FIELD_SEG_NAME = "SEG_NAME";
	public static final String FIELD_TABLE_NAME = "TABLE_NAME";
	public static final String FIELD_SEG_TYPE_NAME = "SEG_TYPE_NAME";
	public static final String FIELD_TABLE_SPACE = "TABLE_SPACE";
	public static final String FIELD_ROW_ID = "ROW_ID";
	public static final String FIELD_USERNAME = "USERNAME";
	public static final String FIELD_SESSION = "SESSION#";
	public static final String FIELD_SERIAL = "SERIAL#";
	public static final String FIELD_SESSION_INFO = "SESSION_INFO";
	public static final String FIELD_THREAD = "THREAD#";
	public static final String FIELD_SEQUENCE = "SEQUENCE#";
	public static final String FIELD_RBASQN = "RBASQN";
	public static final String FIELD_RBABLK = "RSABLK";
	public static final String FIELD_SQL_REDO = "SQL_REDO";
	public static final String FIELD_RS_ID = "RS_ID";
	public static final String FIELD_CSF = "CSF";
	public static final String FIELD_INFO = "INFO";
	public static final String FIELD_STATUS = "STATUS";
	public static final String FIELD_SRC_CON_ID = "SRC_CON_ID";

	public static final String FIELD_CON_ID = "CON_ID";
	public static final String FIELD_OWNER = "OWNER";
	public static final String FIELD_COLUMN_NAME = "COLUMN_NAME";
	public static final String FIELD_NULLABLE = "NULLABLE";
	public static final String FIELD_DATA_TYPE = "DATA_TYPE";
	public static final String FIELD_DATA_SCALE = "DATA_SCALE";
	public static final String FIELD_DATA_PRECISION = "DATA_PRECISION";
	public static final String FIELD_DATA_LENGTH = "DATA_LENGTH";
	public static final String FIELD_PK_COLUMN = "PK_COLUMN";
	public static final String FIELD_UQ_COLUMN = "UQ_COLUMN";

	public static final String TYPE_NUMBER = "NUMBER";
	public static final String TYPE_LONG = "LONG";
	public static final String TYPE_STRING = "STRING";
	public static final String TYPE_TIMESTAMP = "TIMESTAMP";
	public static final String TYPE_DATE = "DATE";
	public static final String TYPE_CHAR = "CHAR";
	public static final String TYPE_FLOAT = "FLOAT";

	public static final String DML_ROW_SCHEMA_NAME = "DML_ROW";
	public static final String FIELD_DATA_ROW = "data";
	public static final String FIELD_BEFORE_DATA_ROW = "before";

	public static final String NULL_FIELD = "NULL";
	public static final String DOT = ".";
	public static final String COMMA = ",";

	public static final String OPERATION_INSERT = "INSERT";
	public static final String OPERATION_UPDATE = "UPDATE";
	public static final String OPERATION_DELETE = "DELETE";
}
