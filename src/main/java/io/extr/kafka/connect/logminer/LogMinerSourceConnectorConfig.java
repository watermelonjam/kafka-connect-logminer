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

// TODO: Add recommenders, good value to be had from dynamic dbconn based ones
public class LogMinerSourceConnectorConfig extends AbstractConfig {
	public static final String DATABASE_GROUP = "Database";
	public static final String CONNECTOR_GROUP = "Connector";
	public static final String LOGMINER_GROUP = "LogMiner";

	public static final String CONNECTION_URL_CONFIG = "connection.url";
	private static final String CONNECTION_URL_DISPLAY = "JDBC URL";
	private static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_CONFIG = "connection.user";
	private static final String CONNECTION_USER_DISPLAY = "JDBC user";
	private static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
	private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC password";
	private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String DB_FETCH_SIZE_CONFIG = "db.fetch.size";
	private static final String DB_FETCH_SIZE_DISPLAY = "Database fetch size";
	public static final long DB_FETCH_SIZE_DEFAULT = 100;
	private static final String DB_FETCH_SIZE_DOC = "Maximum number of rows to include in a single batch "
			+ "when polling for new data. This setting can be used to limit the amount of data buffered "
			+ "internally in the connector.";

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

	public static final String TABLE_POLL_INTERVAL_CONFIG = "table.poll.interval.ms";
	private static final String TABLE_POLL_INTERVAL_DOC = "Frequency in ms to poll for new or removed tables, which may result in updated task "
			+ "configurations to start polling for data in added tables or stop polling for data in "
			+ "removed tables.";
	public static final long TABLE_POLL_INTERVAL_DEFAULT = 60 * 1000;
	private static final String TABLE_POLL_INTERVAL_DISPLAY = "Metadata Change Monitoring Interval (ms)";

	public static final String TOPIC_CONFIG = "topic";
	private static final String TOPIC_DISPLAY = "Topic";
	public static final String TOPIC_DEFAULT = "db.events";
	private static final String TOPIC_DOC = "The name of the Kafka topic to publish data to.";

	public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
	private static final String TOPIC_PREFIX_DISPLAY = "Topic prefix";
	private static final String TOPIC_PREFIX_DOC = "Prefix to prepend to table names to generate the "
			+ "name of the Kafka topic to publish data to.";

	public static final String PARSE_DML_DATA_CONFIG = "parse.dml.data";
	private static final String PARSE_DML_DATA_DISPLAY = "Parse DML data";
	public static final boolean PARSE_DML_DATA_DEFAULT = false;
	private static final String PARSE_DML_DATA_DOC = "Parse DML data into structures";

	public static final String SEEK_SCN_CONFIG = "scn";
	private static final String SEEK_SCN_DISPLAY = "System change number (SCN)";
	private static final String SEEK_SCN_DOC = "The SCN at which to begin reading.  Leave blank to let the "
			+ " connector determine appropriate value using stored Kafka offsets.  Oracle records "
			+ "each database change using this unique sequence number.  LogMiner has access to a range "
			+ "of SCNs that depend on the database configuration (e.g., redo log size).  The connector "
			+ "can be configured to begin reading at a specific SCN (a Long value), the \"min\" (earliest) "
			+ "available SCN, or the \"current\" (latest) available SCN to continue "
			+ "reading.  In the event that the requested SCN is not available, the connector will "
			+ "begin at: \"min\" if a specific SCN is selected "
			+ "that is less than the earliest available; \"max\" if a specific SCN is selected "
			+ "that is greater than the latest available.  In all cases the connector will log its choices.";

	public static final ConfigDef CONFIG_DEF = baseConfigDef();

	public static ConfigDef baseConfigDef() {
		ConfigDef cfg = new ConfigDef();

		initDatabaseConfigGroup(cfg);
		initConnectorConfigGroup(cfg);
		initLogMinerConfigGroup(cfg);

		return cfg;
	}

	private static void initDatabaseConfigGroup(ConfigDef cfg) {
		int orderInGroup = 0;
		cfg.define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC, DATABASE_GROUP,
				orderInGroup++, Width.LONG, CONNECTION_URL_DISPLAY)
				.define(CONNECTION_USER_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_USER_DOC, DATABASE_GROUP,
						orderInGroup++, Width.MEDIUM, CONNECTION_USER_DISPLAY)
				.define(CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, Importance.HIGH, CONNECTION_PASSWORD_DOC,
						DATABASE_GROUP, orderInGroup++, Width.MEDIUM, CONNECTION_PASSWORD_DISPLAY);
	}

	private static void initConnectorConfigGroup(ConfigDef cfg) {
		int orderInGroup = 0;
		cfg.define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.HIGH, TOPIC_DOC, CONNECTOR_GROUP,
				orderInGroup++, Width.SHORT, TOPIC_DISPLAY)
				.define(TOPIC_PREFIX_CONFIG, Type.STRING, Importance.HIGH, TOPIC_PREFIX_DOC, CONNECTOR_GROUP,
						orderInGroup++, Width.MEDIUM, TOPIC_PREFIX_DISPLAY)
				.define(PARSE_DML_DATA_CONFIG, Type.BOOLEAN, Importance.MEDIUM, PARSE_DML_DATA_DOC, CONNECTOR_GROUP,
						orderInGroup++, Width.SHORT, PARSE_DML_DATA_DISPLAY)
				.define(DB_FETCH_SIZE_CONFIG, Type.INT, Importance.HIGH, DB_FETCH_SIZE_DOC, DATABASE_GROUP,
						orderInGroup++, Width.SHORT, DB_FETCH_SIZE_DISPLAY)
				.define(WHITELIST_CONFIG, Type.LIST, Importance.MEDIUM, WHITELIST_DOC, DATABASE_GROUP, orderInGroup++,
						Width.LONG, WHITELIST_DISPLAY)
				.define(BLACKLIST_CONFIG, Type.LIST, Importance.MEDIUM, BLACKLIST_DOC, DATABASE_GROUP, orderInGroup++,
						Width.LONG, BLACKLIST_DISPLAY)
				.define(TABLE_POLL_INTERVAL_CONFIG, Type.LONG, TABLE_POLL_INTERVAL_DEFAULT, Importance.MEDIUM,
						TABLE_POLL_INTERVAL_DOC, CONNECTOR_GROUP, orderInGroup++, Width.SHORT,
						TABLE_POLL_INTERVAL_DISPLAY);
	}

	private static void initLogMinerConfigGroup(ConfigDef cfg) {
		int orderInGroup = 0;
		cfg.define(SEEK_SCN_CONFIG, Type.STRING, Importance.LOW, SEEK_SCN_DOC, LOGMINER_GROUP, orderInGroup++,
				Width.SHORT, SEEK_SCN_DISPLAY);
	}

	public LogMinerSourceConnectorConfig(Map<String, ?> props) {
		super(CONFIG_DEF, props);
	}

	public LogMinerSourceConnectorConfig(ConfigDef configDef, Map<String, ?> props) {
		super(configDef, props);
	}
}
