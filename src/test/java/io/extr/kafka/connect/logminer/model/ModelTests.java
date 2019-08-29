package io.extr.kafka.connect.logminer.model;

import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ModelTests {
	private static final String DATABASE = "ORCLPDB1";
	private static final String OWNER = "OWNER";
	private static final String TABLE = "FOO";

	private static final String TABLE_QNAME = DATABASE + "." + OWNER + "." + TABLE;

	@Test
	public void testTableFromQName() throws Exception {
		Table t = Table.fromQName(TABLE_QNAME);
		assertNotNull(t);
		assertEquals(DATABASE, t.getDatabaseName());
		assertEquals(OWNER, t.getOwnerName());
		assertEquals(TABLE, t.getTableName());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testOffsetFromMap() throws Exception {
		Offset o = Offset.fromMap(Collections.EMPTY_MAP);
		assertNotNull(o);
		assertEquals(new Long(0L), o.getCommitSystemChangeNumber());
	}
}
