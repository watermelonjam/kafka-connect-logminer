package io.x.kafka.connect.logminer.sql;

import java.util.HashMap;
import java.util.Map;

public class LogMinerSQLFactory {
	private static Map<Strategy, LogMinerSQL> INSTANCES;
	
	static {
		INSTANCES = new HashMap<Strategy, LogMinerSQL>();
		INSTANCES.put(Strategy.SINGLE_INSTANCE, new SingleInstanceLogMinerSQL());
		INSTANCES.put(Strategy.MULTITENANT, new MultitenantLogMinerSQL());
	}
	
	private LogMinerSQLFactory() {
	}
	
	public static LogMinerSQL getInstance(Strategy strategy) {
		return INSTANCES.get(strategy);
	}
	
	public enum Strategy { SINGLE_INSTANCE, MULTITENANT	};
}
