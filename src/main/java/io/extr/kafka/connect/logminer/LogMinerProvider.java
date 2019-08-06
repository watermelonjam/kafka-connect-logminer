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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.dialect.BaseLogMinerDialect;
import io.extr.kafka.connect.logminer.dialect.LogMinerSQLFactory;
import io.extr.kafka.connect.logminer.dialect.LogMinerSQLFactory.Strategy;

public class LogMinerProvider implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerProvider.class);

	private static final int VALIDITY_CHECK_TIMEOUT = 5;

	private LogMinerSourceConnectorConfig config;
	private Connection connection;

	public LogMinerProvider(LogMinerSourceConnectorConfig config) {
		this.config = config;
	}

	public synchronized Connection getConnection() throws ConnectException {
		try {
			if (connection == null) {
				initializeConnection();
			} else if (!isValid(connection, VALIDITY_CHECK_TIMEOUT)) {
				LOGGER.info("Database connection is invalid. Reconnecting...");
				close();
				initializeConnection();
			}
		} catch (SQLException e) {
			LOGGER.warn("Connection initialization failure", e);
			throw new ConnectException(e);
		}
		return connection;
	}

	public BaseLogMinerDialect getDialect(Connection connection) throws SQLException {
		if (isMultitenant(connection)) {
			return LogMinerSQLFactory.getInstance(Strategy.MULTITENANT);
		}
		return LogMinerSQLFactory.getInstance(Strategy.SINGLE_INSTANCE);
	}
	
	public boolean isValid(Connection connection, int timeout) throws SQLException {
		if (connection.getMetaData().getJDBCMajorVersion() >= 4) {
			return connection.isValid(timeout);
		}

		try (Statement statement = connection.createStatement()) {
			if (statement.execute("SELECT 1 FROM DUAL")) {
				try (ResultSet rs = statement.getResultSet()) {
				}
			}
		}
		return true;
	}

	public boolean isMultitenant(Connection connection) throws SQLException {
		boolean multitenant = false;
		try (PreparedStatement p = connection
				.prepareStatement("SELECT COUNT(*) FROM DBA_VIEWS WHERE VIEW_NAME = 'CDB_TAB_COLS'")) {
			ResultSet rs = p.executeQuery();
			while (rs.next()) {
				if (rs.getInt(1) == 1) {
					/*
					 * We would hope that the user specifies the CDB in the JDBC URL, but does not
					 * hurt to check. This needs to succeed for logminer to work in multitenant env.
					 */
					try (Statement s = connection.createStatement()) {
						s.execute("ALTER SESSION SET CONTAINER = CDB$ROOT");
						LOGGER.info("Container = CDB$ROOT");
						multitenant = true;
					}
				}
			}
		}
		return multitenant;
	}

	public synchronized void close() {
		if (connection != null) {
			try {
				LOGGER.info("Closing connection");
				connection.close();
			} catch (SQLException e) {
				LOGGER.warn("Ignoring error closing connection", e);
			} finally {
				connection = null;
			}
		}
	}

	private void initializeConnection() throws SQLException {
		String jdbcURL = config.getString(LogMinerSourceConnectorConfig.CONNECTION_URL_CONFIG);
		String user = config.getString(LogMinerSourceConnectorConfig.CONNECTION_USER_CONFIG);
		String password = config.getPassword(LogMinerSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG).value();

		Properties properties = new Properties();
		properties.setProperty("user", user);
		properties.setProperty("password", password);
		LOGGER.debug("Initializing connection to {}", jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, properties);

		DatabaseMetaData md = connection.getMetaData();
		LOGGER.info("Connected to {}", md.getDatabaseProductVersion());
	}
}
