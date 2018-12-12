package io.extr.kafka.connect.logminer.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.util.ConnectorUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.LogMinerSourceConnector;
import io.extr.kafka.connect.logminer.model.TableId;

public class TaskPartitionTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(TaskPartitionTest.class);

	private static List<TableId> tables;

	public TaskPartitionTest() {
	}

	@BeforeClass
	public static void init() {
		tables = new LinkedList<TableId>();
		tables.add(new TableId("CON1", "OWN1", "TAB1", 15L));
		tables.add(new TableId("CON2", "OWN1", "TAB1", 33L));
		tables.add(new TableId("CON3", "OWN1", "TAB1", 0L));
		tables.add(new TableId("CON1", "OWN2", "TAB1", 204L));
		tables.add(new TableId("CON2", "OWN2", "TAB2", 234243L));
		tables.add(new TableId("CON3", "OWN2", "TAB3", 1L));
		tables.add(new TableId("CON1", "OWN3", "TAB1", 13L));
		tables.add(new TableId("CON2", "OWN3", "TAB2", 34343L));
		tables.add(new TableId("CON3", "OWN3", "TAB3", 333L));
	}

	@Test
	public void testSimpleTaskPartition() throws Exception {
		List<List<TableId>> partitions = ConnectorUtils.groupPartitions(tables, 3);
		for (List<TableId> partition : partitions) {
			Assert.assertTrue(partition.size() == 3);
			LOGGER.debug(partition.toString());
		}
	}

	@Test
	public void testPerformanceTaskPartition() throws Exception {
		List<List<TableId>> partitions = LogMinerSourceConnector.distributePartitions(tables, 3);
		for (List<TableId> partition : partitions) {
			Assert.assertTrue(partition.size() == 3);
			LOGGER.debug(partition.toString());
		}
	}
}
