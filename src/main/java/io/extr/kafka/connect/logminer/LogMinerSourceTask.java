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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.dialect.LogMinerDialect;

/**
 * Every instance manages a set of container/owner/table partitions
 * 
 * @author David Arnold
 *
 */
public class LogMinerSourceTask extends SourceTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSourceTask.class);

	private LogMinerSourceTaskConfig config;
	private LogMinerDialect dialect;
	private LogMinerProvider provider;
	private PriorityQueue<LogMinerContentsQuerier> queue = new PriorityQueue<LogMinerContentsQuerier>();
	private final AtomicBoolean running = new AtomicBoolean(false);

	public LogMinerSourceTask() {
	}

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting LogMiner source task");
		try {
			config = new LogMinerSourceTaskConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Cannot start LogMinerSourceTask, configuration error", e);
		}

		try {
			provider = new LogMinerProvider(config);
			Connection connection = provider.getConnection();
			dialect = provider.getDialect(connection);
		} catch (SQLException e) {
			throw new ConnectException("Cannot start LogMinerSourceTask, SQL error", e);
		}

		List<String> tables = config.getList(LogMinerSourceTaskConfig.TABLES_CONFIG);
		List<Map<String, String>> partitions = new ArrayList<>(tables.size());
		Map<String, Map<String, String>> partitionsByTableFqn = new HashMap<>();
		for (String table : tables) {
			Map<String, String> tablePartition = Collections
					.singletonMap(LogMinerSourceConnectorConstants.TABLE_NAME_KEY, table);
			partitions.add(tablePartition);
			partitionsByTableFqn.put(table, tablePartition);
		}
		Map<Map<String, String>, Map<String, Object>> offsets = null;
		offsets = context.offsetStorageReader().offsets(partitions);
		LOGGER.trace("The partition offsets are {}", offsets);

		String topicPrefix = config.getString(LogMinerSourceTaskConfig.TOPIC_PREFIX_CONFIG);
		Map<String, Object> offset = null;
		for (String table : tables) {
			if (offsets != null) {
				Map<String, String> partition = partitionsByTableFqn.get(table);
				offset = offsets.get(partition);
				if (offset != null) {
					LOGGER.info("Found offset {} for partition {}", offsets, partition);
					break;
				}
			}
			queue.add(new LogMinerContentsQuerier(dialect, table, topicPrefix, offset));
		}

		running.set(true);
		LOGGER.info("Started LogMiner source task");
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.trace("{} Polling for new data");

		while (running.get()) {
		}
		return null;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping LogMiner source task");
		running.set(false);
	}

}
