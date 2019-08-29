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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.model.LogMinerEvent;
import io.extr.kafka.connect.logminer.model.Offset;
import io.extr.kafka.connect.logminer.model.Table;

/**
 * Every instance manages a set of [pluggable.]owner.table partitions
 * 
 * @author David Arnold
 *
 */
public class LogMinerSourceTask extends SourceTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSourceTask.class);

	private LogMinerSourceTaskConfig config;
	private LogMinerSession session;
	private final AtomicBoolean running = new AtomicBoolean(false);

	public LogMinerSourceTask() {
	}

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting LogMinerSourceTask instance");
		config = new LogMinerSourceTaskConfig(props);

		try {
			session = new LogMinerSession(config);

			List<String> tables = config.getList(LogMinerSourceTaskConfig.TABLES_CONFIG);
			LOGGER.debug("Configured task tables: {}", tables);
			List<Map<String, String>> partitions = new ArrayList<>(tables.size());
			for (String table : tables) {
				Map<String, String> tablePartition = Collections
						.singletonMap(LogMinerSourceConnectorConstants.TABLE_NAME_KEY, table);
				partitions.add(tablePartition);
			}

			LOGGER.trace("Requesting offsets for partitions: {}", partitions);
			Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(partitions);
			LOGGER.trace("Returned offsets: {}", offsets);

			Map<Table, Offset> state = new HashMap<>();
			if (offsets != null) {
				for (Map<String, String> partition : offsets.keySet()) {
					LOGGER.debug("Setting offset for returned partition {}", partition);
					Table t = Table.fromQName(partition.get(LogMinerSourceConnectorConstants.TABLE_NAME_KEY));
					Offset o = Offset.fromMap(offsets.get(partition));
					state.put(t, o);
				}
			}
			session.start(state);
			running.set(true);
			LOGGER.info("Started LogMinerSourceTask");
		} catch (Exception e) {
			throw new ConnectException("Cannot start LogMinerSourceTask instance", e);
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.debug("Polling for new events");
		try {
			List<LogMinerEvent> events = session.poll();
			if (events == null)
				return null;

			// TODO: either consider implementing topic prefix of some sort, or remove
			// config option in favour of one topic partitioned by table
			return events.stream()
					.map(e -> new SourceRecord(e.getPartition(), e.getOffset(),
							config.getString(LogMinerSourceTaskConfig.TOPIC_CONFIG), e.getSchema(), e.getStruct()))
					.collect(Collectors.toList());
		} catch (Exception e) {
			throw new ConnectException("Error during LogMinerSourceTask poll", e);
		}
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping LogMiner source task");
		running.set(false);
	}
}
