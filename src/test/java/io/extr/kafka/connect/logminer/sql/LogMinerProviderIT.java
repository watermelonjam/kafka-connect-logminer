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

package io.extr.kafka.connect.logminer.sql;

import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.LogMinerProvider;
import io.extr.kafka.connect.logminer.LogMinerSourceConnectorConfig;
import io.extr.kafka.connect.logminer.dialect.LogMinerDialect;
import io.extr.kafka.connect.logminer.model.TableId;

public class LogMinerProviderIT {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerProviderIT.class);

	private static final String JDBC_URL_TEMPLATE = "jdbc:oracle:thin:@%s:%s/KCLDB";

	private Map<String, String> config;

	@Before
	public void initConfig() throws Exception {
		config = new HashMap<String, String>();
		InputStream is = ClassLoader.getSystemResourceAsStream("logminer.source.properties");
		Properties p = new Properties();
		p.load(is);
		for (String key : p.stringPropertyNames()) {
			String value = p.getProperty(key);
			config.put(key, value);
		}

		// Override with system properties from Maven failsafe plugin
		String dbHost = System.getProperty("it-database.host");
		String dbPort = System.getProperty("it-database.port");
		String jdbcURL = String.format(JDBC_URL_TEMPLATE, dbHost, dbPort);
		config.put(LogMinerSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcURL);
	}

	@Test
	public void testDatabaseConnection() throws Exception {
		try (LogMinerProvider provider = new LogMinerProvider(new LogMinerSourceConnectorConfig(config))) {
			Connection connection = provider.getConnection();
			Assert.assertTrue(provider.isMultitenant(connection));
			LogMinerDialect dialect = provider.getDialect(connection);
			List<TableId> tables = dialect.getTables(connection);
			LOGGER.info("Retrieved list of tables: {}", tables);
		}
	}
}
