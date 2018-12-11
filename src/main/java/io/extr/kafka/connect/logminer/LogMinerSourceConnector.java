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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.extr.kafka.connect.logminer.model.TableId;
import io.extr.kafka.connect.logminer.sql.LogMinerSQL;

public class LogMinerSourceConnector extends SourceConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSourceConnector.class);

	private Map<String, String> configProperties;
	private LogMinerSourceConnectorConfig config;
	private LogMinerProvider provider;
	private TableMonitorThread tableMonitorThread;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting LogMiner source connector");
		try {
			configProperties = props;
			config = new LogMinerSourceConnectorConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Cannot start connector, configuration error", e);
		}

		try {
			provider = new LogMinerProvider(config);
			Connection connection = provider.getConnection();
			LogMinerSQL sql = provider.getSQL(connection);
		} catch (SQLException e) {
			throw new ConnectException("Cannot start connector, SQL error", e);
		}

		long tablePollMs = config.getLong(LogMinerSourceConnectorConfig.TODO);
		List<String> whitelist = config.getList(LogMinerSourceConnectorConfig.WHITELIST_CONFIG);
		Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
		List<String> blacklist = config.getList(LogMinerSourceConnectorConfig.BLACKLIST_CONFIG);
		Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);

		if (whitelistSet != null && blacklistSet != null) {
			throw new ConnectException(LogMinerSourceConnectorConfig.WHITELIST_CONFIG + " and "
					+ LogMinerSourceConnectorConfig.BLACKLIST_CONFIG + " are " + "exclusive.");
		}

		tableMonitorThread = new TableMonitorThread(provider, context, 10000L, whitelistSet, blacklistSet);
		tableMonitorThread.start();
	}

	@Override
	public Class<? extends Task> taskClass() {
		return LogMinerSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<TableId> currentTables = tableMonitorThread.tables();
		int numGroups = Math.min(currentTables.size(), maxTasks);

		// TODO Bonus points: KSQL query the output topics of this connector to
		// determine "hot" objects
		List<List<TableId>> tablesGrouped = ConnectorUtils.groupPartitions(currentTables, numGroups);
		
		List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
		for (List<TableId> taskTables : tablesGrouped) {
			Map<String, String> taskProps = new HashMap<>(configProperties);
			ExpressionBuilder builder = dialect.expressionBuilder();
			builder.appendList().delimitedBy(",").of(taskTables);
			taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, builder.toString());
			taskConfigs.add(taskProps);
		}
		log.trace("Task configs with query: {}, tables: {}", taskConfigs, currentTables.toArray());
		return taskConfigs;
		return null;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping LogMiner source connector");
	}

	@Override
	public ConfigDef config() {
		return LogMinerSourceConnectorConfig.CONFIG_DEF;
	}
}
