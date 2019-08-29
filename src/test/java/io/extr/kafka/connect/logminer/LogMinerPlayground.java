package io.extr.kafka.connect.logminer;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.model.LogMinerEvent;
import io.extr.kafka.connect.logminer.model.Offset;
import io.extr.kafka.connect.logminer.model.Table;

public class LogMinerPlayground {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerPlayground.class);

	private LogMinerSourceConnectorConfig config;

	@SuppressWarnings("unchecked")
	@Before
	public void initConfig() throws Exception {
		String configFile = System.getProperty("connector.config.file");

		Object obj = new JSONParser().parse(new FileReader(configFile));
		JSONObject jo = (JSONObject) obj;

		String connectorName = (String) jo.get("name");
		LOGGER.info("Initializing configuration from connector config \"{}\"", connectorName);

		Map<String, String> props = (Map<String, String>) jo.get("config");
		config = new LogMinerSourceConnectorConfig(props);
	}

	@Test
	public void testLogMinerSessionVisibleTables() throws Exception {
		try (LogMinerSession session = new LogMinerSession(config)) {
			Assert.assertTrue(session.isMultitenant());
			List<Table> tables = session.getVisibleTables();
			LOGGER.info("Retrieved list of tables visible in session: {}", tables);

			List<Table> filteredTables = filterTables(tables);
			LOGGER.info("Filtered list of tables: {}", filteredTables);
		}
	}

	@Test
	public void testMining() throws Exception {
		try (LogMinerSession session = new LogMinerSession(config)) {
			List<Table> tables = session.getVisibleTables();
			List<Table> filteredTables = filterTables(tables);

			Map<Table, Offset> state = filteredTables.stream()
					.collect(Collectors.toMap(t -> t, t -> Offset.DEFAULT_OFFSET));

			session.start(state);

			while (true) {
				List<LogMinerEvent> events = session.poll();
				List<SourceRecord> records = events.stream()
						.map(e -> new SourceRecord(e.getPartition(), e.getOffset(),
								config.getString(LogMinerSourceTaskConfig.TOPIC_CONFIG), e.getSchema(), e.getStruct()))
						.collect(Collectors.toList());
				LOGGER.trace("Cooked up {} records for Kafka", records.size());
				Thread.sleep(1000);
			}
		}
	}

	private List<Table> filterTables(List<Table> visibleTables) {
		List<String> whitelist = config.getList(LogMinerSourceConnectorConfig.WHITELIST_CONFIG);
		Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);

		final List<Table> filteredTables = new ArrayList<>(visibleTables.size());
		if (whitelist != null) {
			for (Table table : visibleTables) {
				if (table.matches(whitelistSet)) {
					filteredTables.add(table);
				}
			}
		}
		return filteredTables;
	}
}
