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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.model.Table;

/**
 * Thread that monitors the Oracle dictionary for changes to the set of tables 
 * that this connector should produce redo log events for.
 */
public class DictionaryMonitorThread extends Thread {
	private static final Logger lOGGER = LoggerFactory.getLogger(DictionaryMonitorThread.class);

	private LogMinerSession session;
	private final ConnectorContext context;
	private final CountDownLatch shutdownLatch;
	private final long pollInterval;
	private Set<String> whitelist;
	private Set<String> blacklist;
	private List<Table> tables;

	public DictionaryMonitorThread(LogMinerSession session, ConnectorContext context, long pollInterval,
			Set<String> whitelist, Set<String> blacklist) {
		this.session = session;
		this.context = context;
		this.shutdownLatch = new CountDownLatch(1);
		this.pollInterval = pollInterval;
		this.whitelist = whitelist;
		this.blacklist = blacklist;
		this.tables = null;
	}

	@Override
	public void run() {
		lOGGER.info("Starting thread to monitor database tables");
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
				lOGGER.debug("Waiting {} ms to check for dictionary changes", pollInterval);
				boolean shuttingDown = shutdownLatch.await(pollInterval, TimeUnit.MILLISECONDS);
				if (shuttingDown) {
					return;
				}
			} catch (InterruptedException e) {
				lOGGER.warn("Unexpected InterruptedException, ignoring", e);
			}
		}
	}

	public synchronized List<Table> tables() {
		// TODO: Timeout should probably be user-configurable or class-level constant
		final long timeout = 10000L;
		long started = System.currentTimeMillis();
		long now = started;
		while (tables == null && now - started < timeout) {
			try {
				wait(timeout - (now - started));
			} catch (InterruptedException e) {
				lOGGER.warn("Unexpected InterruptedException, ignoring", e);
			}
			now = System.currentTimeMillis();
		}
		if (tables == null) {
			throw new ConnectException("Tables could not be updated quickly enough");
		}
		return tables;
	}

	public void shutdown() {
		lOGGER.info("Shutting down thread monitoring tables");
		session.close();
		shutdownLatch.countDown();
	}

	private synchronized boolean updateTables() {
		final List<Table> tables;
		try {
			tables = session.getVisibleTables();
			lOGGER.trace("Monitor thread found tables: " + Arrays.toString(tables.toArray()));
		} catch (Exception e) {
			lOGGER.error("Error while trying to get updated table list, ignoring and waiting for next table poll"
					+ " interval", e);
			return false;
		}

		final List<Table> filteredTables = new ArrayList<>(tables.size());
		if (whitelist != null) {
			for (Table table : tables) {
				if (table.matches(whitelist)) {
					filteredTables.add(table);
				}
			}
		} else if (blacklist != null) {
			for (Table table : tables) {
				if (!table.matches(blacklist)) {
					filteredTables.add(table);
				}
			}
		} else {
			filteredTables.addAll(tables);
		}

		// TODO: enrich filteredTables with event stats from our own topic
		
		if (!filteredTables.equals(this.tables)) {
			lOGGER.info("Monitor thread filtered tables: {}", filteredTables);
			List<Table> previousTables = this.tables;
			this.tables = filteredTables;
			notifyAll();
			// Only return true if the table list wasn't previously null, i.e. if this was
			// not the first table lookup
			return previousTables != null;
		}

		return false;
	}
}
