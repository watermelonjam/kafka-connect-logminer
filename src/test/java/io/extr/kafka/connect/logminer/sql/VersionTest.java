package io.extr.kafka.connect.logminer.sql;

import org.junit.Assert;
import org.junit.Test;

import io.extr.kafka.connect.logminer.Version;

public class VersionTest {
	@Test
	public void testVersion() throws Exception {
		Assert.assertNotNull(Version.getVersion());
		Assert.assertNotEquals("unknown", Version.getVersion());
	}
}
