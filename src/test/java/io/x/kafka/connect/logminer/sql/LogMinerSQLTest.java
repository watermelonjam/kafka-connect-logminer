package io.x.kafka.connect.logminer.sql;

import org.junit.Test;

import io.x.kafka.connect.logminer.sql.LogMinerSQL.Statement;
import io.x.kafka.connect.logminer.sql.LogMinerSQLFactory.Strategy;

public class LogMinerSQLTest {
	@Test
	public void testMultitenantLogMinerSQL() throws Exception {
		LogMinerSQL sql = LogMinerSQLFactory.getInstance(Strategy.MULTITENANT);
		String statement = sql.getStatement(Statement.DICTIONARY);
		System.out.println(statement);
		statement = sql.getStatement(Statement.START_MINING);
		System.out.println(statement);
	}

	@Test
	public void testSingleInstanceLogMinerSQL() throws Exception {
		LogMinerSQL sql = LogMinerSQLFactory.getInstance(Strategy.SINGLE_INSTANCE);
		String statement = sql.getStatement(Statement.DICTIONARY);
		System.out.println(statement);
		statement = sql.getStatement(Statement.START_MINING);
		System.out.println(statement);
	}
}
