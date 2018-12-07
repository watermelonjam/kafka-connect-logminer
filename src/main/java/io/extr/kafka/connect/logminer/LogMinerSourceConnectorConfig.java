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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class LogMinerSourceConnectorConfig extends AbstractConfig {
	public static final String LOGMINER_GROUP = "LogMiner";
	public static final String CONNECTOR_GROUP = "Connector";
	public static final String DATABASE_GROUP = "Database";

	public static final String LOGMINER_DIALECT_CONFIG = "dialect";
	private static final String LOGMINER_DIALECT_DISPLAY = "Dialect";
	public static final String LOGMINER_DIALECT_DEFAULT = "single";
	private static final String LOGMINER_DIALECT_DOC = "The LogMiner dialect to use when mining the database.  Possible "
			+ "values include \"single\" and \"multitenant\"";

	public static final String TOPIC_CONFIG = "topic";
	private static final String TOPIC_DISPLAY = "Topic";
	public static final String TOPIC_DEFAULT = "db.events";
	private static final String TOPIC_DOC = "";

	public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
	private static final String TOPIC_PREFIX_DISPLAY = "Topic prefix";
	private static final String TOPIC_PREFIX_DOC = "";

	public static final String CONNECTION_URL_CONFIG = "connection.url";
	private static final String CONNECTION_URL_DISPLAY = "JDBC URL";
	private static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_CONFIG = "connection.user";
	private static final String CONNECTION_USER_DISPLAY = "JDBC user";
	private static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
	private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC password";
	private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String WHITELIST_CONFIG = "table.whitelist";
	private static final String WHITELIST_DISPLAY = "Table whitelist";
	public static final String WHITELIST_DEFAULT = "";
	private static final String WHITELIST_DOC = "List of tables to include in mining. If specified,"
			+ " table.blacklist may not be set.";

	public static final String BLACKLIST_CONFIG = "table.blacklist";
	private static final String BLACKLIST_DISPLAY = "Table blacklist";
	public static final String BLACKLIST_DEFAULT = "";
	private static final String BLACKLIST_DOC = "List of tables to exclude from mining. If specified,"
			+ " table.whitelist may not be set.";

	public static final String PARSE_DML_DATA_CONFIG = "parse.dml.data";
	private static final String PARSE_DML_DATA_DISPLAY = "Parse DML data";
	public static final boolean PARSE_DML_DATA_DEFAULT = false;
	private static final String PARSE_DML_DATA_DOC = "Parse DML data into structures";

	public static final String DB_FETCH_SIZE_CONFIG = "db.fetch.size";
	private static final String DB_FETCH_SIZE_DISPLAY = "Database fetch size";
	public static final long DB_FETCH_SIZE_DEFAULT = 100L;
	private static final String DB_FETCH_SIZE_DOC = "Maximum number of rows to include in a single batch "
			+ "when polling for new data. This setting can be used to limit the amount of data buffered "
			+ "internally in the connector.";

	public static final String RESET_OFFSET_CONFIG = "reset.offset";
	private static final String RESET_OFFSET_DISPLAY = "Reset offset";
	public static final boolean RESET_OFFSET_DEFAULT = false;
	private static final String RESET_OFFSET_DOC = "Reset offset";

	public static final String START_SCN_CONFIG = "start.scn";
	private static final String START_SCN_DISPLAY = "Start system change number";
	private static final String START_SCN_DOC = "Start scn";

	public static final String DB_NAME_ALIAS_CONFIG = "db.name.alias";
	private static final String DB_NAME_ALIAS_DISPLAY = "Database name alias";
	private static final String DB_NAME_ALIAS_DOC = "Database alias";

	public static final ConfigDef CONFIG_DEF = initConfig();

	public static ConfigDef initConfig() {
		ConfigDef cfg = new ConfigDef();

		int orderInGroup = 0;
		cfg.define(LOGMINER_DIALECT_CONFIG, Type.STRING, LOGMINER_DIALECT_DEFAULT, Importance.HIGH,
				LOGMINER_DIALECT_DOC, LOGMINER_GROUP, orderInGroup++, Width.MEDIUM, LOGMINER_DIALECT_DISPLAY);
		initConnectorConfig(cfg);
		return cfg;
	}

	private static void initConnectorConfig(ConfigDef cfg) {
		int orderInGroup = 0;
		cfg.define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.HIGH, TOPIC_DOC, CONNECTOR_GROUP,
				orderInGroup++, Width.SHORT, TOPIC_DISPLAY);
	}
	
	public LogMinerSourceConnectorConfig(Map<String, ?> props) {
		super(CONFIG_DEF, props);
	}
}
