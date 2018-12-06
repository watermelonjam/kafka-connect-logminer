package io.x.kafka.connect.logminer.sql;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleInstanceLogMinerSQL extends LogMinerSQL {
	private static final Logger LOGGER = LoggerFactory.getLogger(SingleInstanceLogMinerSQL.class);

	private static final String PROPERTIES_FILE = "sql.single.properties";

	private static Map<Statement, String> STATEMENTS;

	static {
		STATEMENTS = new HashMap<Statement, String>();
		try {
			initializeStatements(STATEMENTS, PROPERTIES_FILE);
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	@Override
	public String getStatement(Statement statement) {
		if (STATEMENTS.containsKey(statement)) {
			return STATEMENTS.get(statement);
		}
		return super.getStatement(statement);
	}

}
