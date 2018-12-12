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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class LogMinerSourceTaskConfig extends LogMinerSourceConnectorConfig {
	public static final String TABLES_CONFIG = "tables";
	private static final String TABLES_DOC = "List of tables for this task to watch for changes.";

	static ConfigDef config = baseConfigDef().define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC);

	public LogMinerSourceTaskConfig(Map<String, String> props) {
		super(config, props);
	}
}
