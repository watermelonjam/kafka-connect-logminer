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

	public static final String LOGMINER_DIALECT_CONFIG = "dialect";
	private static final String LOGMINER_DIALECT_DISPLAY = "Dialect";
	private static final String LOGMINER_DIALECT_DEFAULT = "single";
	private static final String LOGMINER_DIALECT_DOC = "The LogMiner dialect to use when mining the database.  Possible "
			+ "values include \"single\" and \"multitenant\"";

	public static final ConfigDef CONFIG_DEF = initConfig();

	public static ConfigDef initConfig() {
		ConfigDef cfg = new ConfigDef();

		int orderInGroup = 0;
		cfg.define(LOGMINER_DIALECT_CONFIG, Type.STRING, LOGMINER_DIALECT_DEFAULT, Importance.HIGH,
				LOGMINER_DIALECT_DOC, LOGMINER_GROUP, orderInGroup++, Width.MEDIUM, LOGMINER_DIALECT_DISPLAY);
		return cfg;
	}

	public LogMinerSourceConnectorConfig(Map<String, ?> props) {
		super(CONFIG_DEF, props);
	}
}
