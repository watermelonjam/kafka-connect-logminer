/**
 * Copyright 2015 Confluent Inc.
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.model.TableId;

/**
 * Thread that monitors the database for changes to the set of tables in the
 * database that this connector should produce redo logs for.
 */
public class TableMonitorThread extends Thread {
	private static final Logger lOGGER = LoggerFactory.getLogger(TableMonitorThread.class);

	private final LogMinerProvider provider;
	private final ConnectorContext context;
	private final CountDownLatch shutdownLatch;
	private final long pollInterval;
	private Set<String> whitelist;
	private Set<String> blacklist;
	private List<TableId> tables;

	public TableMonitorThread(LogMinerProvider provider, ConnectorContext context, long pollInterval,
			Set<String> whitelist, Set<String> blacklist) {
		this.provider = provider;
		this.context = context;
		this.shutdownLatch = new CountDownLatch(1);
		this.pollInterval = pollInterval;
		this.whitelist = whitelist;
		this.blacklist = blacklist;
		this.tables = null;
	}

	@Override
	public void run() {
		lOGGER.info("Starting thread to monitor tables.");
		while (shutdownLatch.getCount() > 0) {
			try {
				if (updateTables()) {
					context.requestTaskReconfiguration();
				}
			} catch (Exception e) {
				context.raiseError(e);
				throw e;
			}

			try {
				lOGGER.debug("Waiting {} ms to check for table changes", pollInterval);
				boolean shuttingDown = shutdownLatch.await(pollInterval, TimeUnit.MILLISECONDS);
				if (shuttingDown) {
					return;
				}
			} catch (InterruptedException e) {
				lOGGER.error("Unexpected InterruptedException, ignoring: ", e);
			}
		}
	}

	public synchronized List<TableId> tables() {
		// TODO: Timeout should probably be user-configurable or class-level constant
		final long timeout = 10000L;
		long started = System.currentTimeMillis();
		long now = started;
		while (tables == null && now - started < timeout) {
			try {
				wait(timeout - (now - started));
			} catch (InterruptedException e) {
				// Ignore
			}
			now = System.currentTimeMillis();
		}
		if (tables == null) {
			throw new ConnectException("Tables could not be updated quickly enough.");
		}
		return tables;
	}

	public void shutdown() {
		lOGGER.info("Shutting down thread monitoring tables.");
		shutdownLatch.countDown();
	}

	private synchronized boolean updateTables() {
		final List<TableId> tables;
		try {
			tables = provider.getTables();
			lOGGER.debug("Got the following tables: " + Arrays.toString(tables.toArray()));
		} catch (SQLException e) {
			lOGGER.error("Error while trying to get updated table list, ignoring and waiting for next table poll"
					+ " interval", e);
			provider.close();
			return false;
		}

		final List<TableId> filteredTables = new ArrayList<>(tables.size());
		if (whitelist != null) {
			for (TableId table : tables) {
				String fqn1 = dialect.expressionBuilder().append(table, false).toString();
				String fqn2 = dialect.expressionBuilder().append(table, true).toString();
				if (whitelist.contains(fqn1) || whitelist.contains(fqn2) || whitelist.contains(table.tableName())) {
					filteredTables.add(table);
				}
			}
		} else if (blacklist != null) {
			for (TableId table : tables) {
				String fqn1 = dialect.expressionBuilder().append(table, false).toString();
				String fqn2 = dialect.expressionBuilder().append(table, true).toString();
				if (!(blacklist.contains(fqn1) || blacklist.contains(fqn2) || blacklist.contains(table.tableName()))) {
					filteredTables.add(table);
				}
			}
		} else {
			filteredTables.addAll(tables);
		}

		if (!filteredTables.equals(this.tables)) {
			lOGGER.info("After filtering the tables are: {}",
					dialect.expressionBuilder().appendList().delimitedBy(",").of(filteredTables));
			Map<String, List<TableId>> duplicates = filteredTables.stream()
					.collect(Collectors.groupingBy(TableId::tableName)).entrySet().stream()
					.filter(entry -> entry.getValue().size() > 1)
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			this.duplicates = duplicates;
			List<TableId> previousTables = this.tables;
			this.tables = filteredTables;
			notifyAll();
			// Only return true if the table list wasn't previously null, i.e. if this was
			// not the
			// first table lookup
			return previousTables != null;
		}

		return false;
	}
}
