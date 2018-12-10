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
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.sql.LogMinerSQL;

public class LogMinerSourceConnector extends SourceConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSourceConnector.class);
	
	private Map<String, String> configProperties;
	private LogMinerSourceConnectorConfig config;
	private LogMinerProvider provider;
	
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
		
		// TODO: parse white/black lists and validate selections
		// TODO: create monitorthread and start it
	}

	@Override
	public Class<? extends Task> taskClass() {
		return LogMinerSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// TODO Auto-generated method stub
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
