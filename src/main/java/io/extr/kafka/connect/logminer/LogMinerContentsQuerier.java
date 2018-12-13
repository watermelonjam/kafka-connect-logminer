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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.dialect.LogMinerDialect;
import io.extr.kafka.connect.logminer.model.TableId;

public class LogMinerContentsQuerier {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerContentsQuerier.class);

	private final LogMinerDialect dialect;
	private final String topicPrefix;
	private final TableId tableId;
	private final Long offset;

	public LogMinerContentsQuerier(LogMinerDialect dialect, String tableFQN, String topicPrefix, Map<String, Object> offset) {
		this.dialect = dialect;
		this.topicPrefix = topicPrefix;
		this.tableId = TableId.fromFQN(tableFQN);
		this.offset = (Long) offset.get(LogMinerSourceConnectorConstants.FIELD_SCN);
	}

}
