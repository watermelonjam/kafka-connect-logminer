package io.extr.kafka.connect.logminer;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.extr.kafka.connect.logminer.dialect.BaseLogMinerDialect;
import io.extr.kafka.connect.logminer.dialect.LogMinerDialect;
import io.extr.kafka.connect.logminer.dialect.LogMinerSQLFactory;
import io.extr.kafka.connect.logminer.dialect.LogMinerSQLFactory.Strategy;
import io.extr.kafka.connect.logminer.model.LogMinerEvent;
import io.extr.kafka.connect.logminer.model.Offset;
import io.extr.kafka.connect.logminer.model.Table;
import net.sf.jsqlparser.JSQLParserException;

public class LogMinerSession implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSession.class);

	private static final int VALIDITY_CHECK_TIMEOUT = 5;

	private static final String SEEK_SCN_MINIMUM = "min";
	private static final String SEEK_SCN_CURRENT = "current";

	private LogMinerSourceConnectorConfig config;
	private Connection connection;
	private Boolean multitenant;
	private PreparedStatement miningQuery;
	private PreparedStatement dictionaryQuery;
	private ResultSet rs;
	private Map<Table, Schema> schemas = new HashMap<>();
	private ExecutorService executorService;
	private FutureTask<Void> miningQueryTask;

	private Boolean started;

	public LogMinerSession(LogMinerSourceConnectorConfig config) {
		this.config = config;
		this.executorService = Executors.newCachedThreadPool();
	}

	public synchronized void start(Map<Table, Offset> state) throws SQLException {
		Long sessionStartSCN = getSessionStartSCN(state);

		LOGGER.info("Starting log miner session at SCN {}", sessionStartSCN);
		CallableStatement s = getConnection()
				.prepareCall(getDialect().getStatement(LogMinerDialect.Statement.START_MINING));
		s.setLong(1, sessionStartSCN);
		s.execute();

		dictionaryQuery = getConnection()
				.prepareStatement(getDialect().getStatement(LogMinerDialect.Statement.DICTIONARY));

		miningQuery = createMiningQuery(state);

		LOGGER.debug("Starting mining query thread");
		Callable<Void> queryCall = new Callable<Void>() {
			public Void call() throws SQLException {
				LOGGER.debug("Mining query execution start");
				rs = miningQuery.executeQuery();
				LOGGER.debug("Mining query ResultSet assigned");
				return null;
			}
		};
		miningQueryTask = new FutureTask<Void>(queryCall);
		executorService.execute(miningQueryTask);

		started = Boolean.TRUE;
		LOGGER.debug("Log miner session started");
	}

	public synchronized void close() {
		try {
			if (started) {
				LOGGER.info("Stopping log miner session");

				if (miningQueryTask != null && !miningQueryTask.isDone()) {
					LOGGER.debug("Shutting down query thread");
					miningQueryTask.cancel(true);
				}

				LOGGER.debug("Closing session JDBC resources");
				rs.close();

				miningQuery.cancel();
				miningQuery.close();

				dictionaryQuery.cancel();
				dictionaryQuery.close();

				CallableStatement s = connection
						.prepareCall(getDialect().getStatement(LogMinerDialect.Statement.STOP_MINING));
				s.execute();
				s.close();
				LOGGER.debug("Log miner session ended");
			}

			if (connection != null) {
				LOGGER.info("Closing database connection");
				connection.close();
			}
		} catch (SQLException e) {
			LOGGER.warn("Ignoring error closing session JDBC resources", e);
		} finally {
			connection = null;
		}
	}

	public List<LogMinerEvent> poll() throws SQLException {
		List<LogMinerEvent> events = new ArrayList<>();
		while (rs != null && rs.next()) {
			if (LOGGER.isTraceEnabled()) {
				logRawMinerData();
			}

			String redoSQL = rs.getString(LogMinerSourceConnectorConstants.FIELD_SQL_REDO);
			if (redoSQL.contains(LogMinerSourceConnectorConstants.TEMPORARY_TABLES_PATTERN))
				continue;

			Boolean continuation = rs.getBoolean(LogMinerSourceConnectorConstants.FIELD_CSF);
			while (continuation) {
				rs.next();
				redoSQL += rs.getString(LogMinerSourceConnectorConstants.FIELD_SQL_REDO);
				continuation = rs.getBoolean(LogMinerSourceConnectorConstants.FIELD_CSF);
			}

			String databaseName = rs.getString(LogMinerSourceConnectorConstants.FIELD_SRC_CON_NAME);
			String ownerName = rs.getString(LogMinerSourceConnectorConstants.FIELD_SEG_OWNER);
			String tableName = rs.getString(LogMinerSourceConnectorConstants.FIELD_TABLE_NAME);

			LogMinerEvent event = createEvent(new Table(databaseName, ownerName, tableName), redoSQL);

			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_SCN, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_ROW_ID, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_SEG_OWNER, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_TABLE_NAME, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_TIMESTAMP, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_SQL_REDO, rs);
			populateEventField(event, LogMinerSourceConnectorConstants.FIELD_OPERATION, rs);

			LOGGER.debug("Poll added event: {}", event.toString());
			events.add(event);
			LOGGER.debug("ResultSet simple poll thread returned {} events", events.size());
			return events;
		}
		
		return null;
	}

	private void populateEventField(LogMinerEvent event, String field, ResultSet rs) throws SQLException {
		Object o = convertFieldValue(rs.getString(field), event.getSchema().field(field).schema());
		event.getStruct().put(field, o);
	}

	public List<Table> getVisibleTables() throws SQLException {
		LOGGER.trace("Retrieving list of tables visible in log miner session");
		return getDialect().getTables(getConnection());
	}

	public boolean isMultitenant() throws SQLException {
		if (multitenant != null) {
			return multitenant;
		}

		try (PreparedStatement p = getConnection()
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
						LOGGER.debug("Set session multitenant container = CDB$ROOT");
						multitenant = Boolean.TRUE;
					}
				}
			}
		}
		return multitenant;
	}

	private PreparedStatement createMiningQuery(Map<Table, Offset> state) throws SQLException {
		String baseMiningStatement = getDialect().getStatement(LogMinerDialect.Statement.CONTENTS);

		StringBuffer where = new StringBuffer("(");
		Iterator<Table> it = state.keySet().iterator();
		while (it.hasNext()) {
			Table table = it.next();
			where.append("(SRC_CON_NAME = ? AND SEG_OWNER = ? AND TABLE_NAME = ? AND COMMIT_SCN >= ?)");
			LOGGER.trace("Added mining query WHERE condition for {}", table.getQName());
			if (it.hasNext()) {
				where.append(" OR ");
			}
		}
		where.append(")");

		String completedMiningQuery = baseMiningStatement + where.toString();
		LOGGER.debug("Completed session mining query: {}", completedMiningQuery);

		int fetchSize = config.getInt(LogMinerSourceConnectorConfig.DB_FETCH_SIZE_CONFIG);
		PreparedStatement ps = connection.prepareCall(completedMiningQuery);
		ps.setFetchSize(fetchSize);
		LOGGER.debug("Set mining query fetch size = {}", fetchSize);

		int paramIdx = 0;
		Iterator<Table> it2 = state.keySet().iterator();
		while (it2.hasNext()) {
			Table table = it2.next();
			Offset offset = state.get(table);
			ps.setString(++paramIdx, table.getDatabaseName());
			ps.setString(++paramIdx, table.getOwnerName());
			ps.setString(++paramIdx, table.getTableName());
			ps.setLong(++paramIdx, offset.getCommitSystemChangeNumber());

			LOGGER.trace("Set mining query WHERE parameters for {} @ SCN {}", table.getQName(),
					offset.getCommitSystemChangeNumber());
		}

		return ps;
	}

	private synchronized Connection getConnection() throws ConnectException {
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

	private BaseLogMinerDialect getDialect() throws SQLException {
		if (isMultitenant()) {
			return LogMinerSQLFactory.getInstance(Strategy.MULTITENANT);
		}
		return LogMinerSQLFactory.getInstance(Strategy.SINGLE_INSTANCE);
	}

	private boolean isValid(Connection connection, int timeout) throws SQLException {
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

	/**
	 * Use the explicit config SCN setting, or default to minimum of supplied state
	 * offsets
	 * 
	 * @param state
	 * @return starting system change number
	 * @throws SQLException
	 * @throws ConnectException
	 */
	private Long getSessionStartSCN(Map<Table, Offset> state) throws ConnectException, SQLException {
		Long currentSCN = getCurrentSCN();
		Long minimumSCN = getMinimumSCN();

		/* Consider connector config specifications first */
		String seekSCN = config.getString(LogMinerSourceConnectorConfig.SEEK_SCN_CONFIG);
		if (SEEK_SCN_MINIMUM.equals(seekSCN)) {
			LOGGER.debug("Session start from earliest SCN = {}", minimumSCN);
			return minimumSCN;
		}
		if (SEEK_SCN_CURRENT.equals(seekSCN)) {
			LOGGER.debug("Session start from latest SCN = {}", currentSCN);
			return currentSCN;
		}

		Long requestedSCN = 0L;
		if (seekSCN != null) {
			try {
				requestedSCN = Long.parseLong(seekSCN);
			} catch (NumberFormatException nfe) {
				LOGGER.warn("Session start invalid SCN \"{}\" specified in configuration, using current = {}", seekSCN,
						currentSCN);
				return currentSCN;
			}
		} else {
			requestedSCN = getMinimumOffsetSCN(state);
		}
		if (requestedSCN == 0L) {
			/* No setting, no state, start at minimum available */
			LOGGER.debug("Session start defaulting to earliest SCN = {}", minimumSCN);
			return minimumSCN;
		}

		Long scn = requestedSCN;
		PreparedStatement st = getConnection()
				.prepareStatement(getDialect().getStatement(LogMinerDialect.Statement.LATEST_SCN));
		st.setLong(1, requestedSCN);
		st.setLong(2, requestedSCN);
		try (ResultSet scnResult = st.executeQuery()) {
			while (scnResult.next()) {
				scn = scnResult.getLong(1);
			}
		}

		LOGGER.debug("Session start calculated SCN = {} from requested SCN = {}", scn, requestedSCN);
		return scn;
	}

	private Long getCurrentSCN() throws SQLException {
		Long scn = 0L;

		PreparedStatement st = getConnection()
				.prepareStatement(getDialect().getStatement(LogMinerDialect.Statement.CURRENT_SCN));
		try (ResultSet r = st.executeQuery()) {
			while (r.next()) {
				scn = r.getLong(1);
			}
		}

		return scn;
	}

	private Long getMinimumSCN() throws SQLException {
		Long scn = 0L;

		PreparedStatement st = getConnection()
				.prepareStatement(getDialect().getStatement(LogMinerDialect.Statement.EARLIEST_SCN));
		try (ResultSet r = st.executeQuery()) {
			while (r.next()) {
				scn = r.getLong(1);
			}
		}

		return scn;
	}

	private Long getMinimumOffsetSCN(Map<Table, Offset> state) {
		Optional<Offset> possibleMinimum = state.values().stream()
				.min((offset1, offset2) -> offset1.getSystemChangeNumber().compareTo(offset2.getSystemChangeNumber()));

		return possibleMinimum.isPresent() ? possibleMinimum.get().getSystemChangeNumber() : 0L;
	}

	private void logRawMinerData() throws SQLException {
		StringBuffer b = new StringBuffer();
		for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
			String columnName = rs.getMetaData().getColumnName(i);
			Object columnValue = rs.getObject(i);
			b.append("[" + columnName + "=" + (columnValue == null ? "NULL" : columnValue.toString()) + "]");
		}
		LOGGER.trace(b.toString());
	}

	private LogMinerEvent createEvent(Table table, String redoSql) throws SQLException {
		Schema rowSchema;
		if (schemas.containsKey(table)) {
			rowSchema = schemas.get(table);
			LOGGER.trace("{} retrieved from cache", rowSchema.toString());
		} else {
			rowSchema = createRowSchema(table);
			schemas.put(table, rowSchema);
			LOGGER.info("{} created and cached", rowSchema.toString());
		}

		Schema eventSchema = SchemaBuilder.struct()
				.name(table.getQName() + LogMinerSourceConnectorConstants.EVENT_SCHEMA_QUALIFIER)
				.field(LogMinerSourceConnectorConstants.FIELD_SCN, Schema.INT64_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN, Schema.INT64_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_ROW_ID, Schema.STRING_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_SEG_OWNER, Schema.STRING_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_TABLE_NAME, Schema.STRING_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP, org.apache.kafka.connect.data.Timestamp.SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_SQL_REDO, Schema.STRING_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_OPERATION, Schema.STRING_SCHEMA)
				.field(LogMinerSourceConnectorConstants.FIELD_BEFORE_DATA_ROW, rowSchema)
				.field(LogMinerSourceConnectorConstants.FIELD_AFTER_DATA_ROW, rowSchema).build();
		LOGGER.trace("{} created", eventSchema.toString());

		Struct eventStruct = createEventStruct(rowSchema, eventSchema, redoSql);

		return new LogMinerEvent(eventSchema, eventStruct);
	}

	private Schema createRowSchema(Table table) throws SQLException {
		LOGGER.debug("Creating schema for {}", table.getQName());
		SchemaBuilder structBuilder = SchemaBuilder.struct()
				.name(table.getQName() + LogMinerSourceConnectorConstants.ROW_SCHEMA_QUALIFIER);
		// TODO: consider using dictionary LAST_DDL_TIME to Integer magic to set schema
		// version

		dictionaryQuery.setString(1, table.getDatabaseName());
		dictionaryQuery.setString(2, table.getOwnerName());
		dictionaryQuery.setString(3, table.getTableName());

		try (ResultSet drs = dictionaryQuery.executeQuery()) {
			if (drs.isBeforeFirst()) {
				// TODO: No result, charf
			}
			while (drs.next()) {
				String columnName = drs.getString(LogMinerSourceConnectorConstants.FIELD_COLUMN_NAME);
				Boolean nullable = drs.getString(LogMinerSourceConnectorConstants.FIELD_NULLABLE).equals("Y") ? true
						: false;
				String dataType = drs.getString(LogMinerSourceConnectorConstants.FIELD_DATA_TYPE);
				if (dataType.contains(LogMinerSourceConnectorConstants.TYPE_TIMESTAMP))
					dataType = LogMinerSourceConnectorConstants.TYPE_TIMESTAMP;
				int dataLength = drs.getInt(LogMinerSourceConnectorConstants.FIELD_DATA_LENGTH);
				int dataScale = drs.getInt(LogMinerSourceConnectorConstants.FIELD_DATA_SCALE);
				int dataPrecision = drs.getInt(LogMinerSourceConnectorConstants.FIELD_DATA_PRECISION);
				Boolean pkColumn = drs.getInt(LogMinerSourceConnectorConstants.FIELD_PK_COLUMN) == 1 ? true : false;
				Boolean uqColumn = drs.getInt(LogMinerSourceConnectorConstants.FIELD_UQ_COLUMN) == 1 ? true : false;
				Schema columnSchema = null;

				switch (dataType) {
				case LogMinerSourceConnectorConstants.TYPE_NUMBER: {
					if (dataScale > 0 || dataPrecision == 0) {
						columnSchema = nullable ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
					} else {
						switch (dataPrecision) {
						case 1:
						case 2:
							columnSchema = nullable ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
							break;
						case 3:
						case 4:
							columnSchema = nullable ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
							break;
						case 5:
						case 6:
						case 7:
						case 8:
						case 9:
							columnSchema = nullable ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
							break;
						default:
							columnSchema = nullable ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
							break;
						}
					}
					break;
				}
				case "CHAR":
				case "VARCHAR":
				case "VARCHAR2":
				case "NCHAR":
				case "NVARCHAR":
				case "NVARCHAR2":
				case "LONG":
				case "CLOB": {
					columnSchema = nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
					break;
				}
				case LogMinerSourceConnectorConstants.TYPE_DATE:
				case LogMinerSourceConnectorConstants.TYPE_TIMESTAMP: {
					columnSchema = nullable ? LogMinerSourceConnectorConstants.SCEMA_OPTIONAL_TIMESTAMP
							: LogMinerSourceConnectorConstants.SCHEMA_TIMESTAMP;
					break;
				}
				default:
					columnSchema = nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
					break;
				}
				structBuilder.field(columnName, columnSchema);
			}
		}

		return structBuilder.build();
	}

	private Struct createEventStruct(Schema rowSchema, Schema eventSchema, String redoSql) throws SQLException {
		try {
			Map<String, Map<String, String>> changes = LogMinerSQLParser.parseRedoSQL(redoSql);

			Struct before = createDataStruct(rowSchema,
					changes.get(LogMinerSourceConnectorConstants.FIELD_BEFORE_DATA_ROW));
			Struct after = createDataStruct(rowSchema,
					changes.get(LogMinerSourceConnectorConstants.FIELD_AFTER_DATA_ROW));

			Struct event = new Struct(eventSchema);
			event.put(LogMinerSourceConnectorConstants.FIELD_BEFORE_DATA_ROW, before);
			event.put(LogMinerSourceConnectorConstants.FIELD_AFTER_DATA_ROW, after);
			LOGGER.trace("Created event {}", event.toString());
			return event;
		} catch (JSQLParserException e) {
			throw new SQLException("Cannot parse log miner redo SQL", e);
		}
	}

	private Struct createDataStruct(Schema schema, Map<String, String> data) {
		Struct dataStruct = new Struct(schema);
		for (String field : data.keySet()) {
			String value = data.get(field);
			Schema fieldSchema = schema.field(field).schema();
			dataStruct.put(field, convertFieldValue(value, fieldSchema));
		}
		return dataStruct;
	}

	private Object convertFieldValue(String value, Schema fieldSchema) {
		if (fieldSchema == null || fieldSchema.equals(Schema.STRING_SCHEMA)) {
			return value;
		}
		if (fieldSchema.equals(Schema.INT8_SCHEMA)) {
			return Byte.parseByte(value);
		}
		if (fieldSchema.equals(Schema.INT16_SCHEMA)) {
			return Short.parseShort(value);
		}
		if (fieldSchema.equals(Schema.INT32_SCHEMA)) {
			return Integer.parseInt(value);
		}
		if (fieldSchema.equals(Schema.INT64_SCHEMA)) {
			return Long.parseLong(value);
		}
		if (fieldSchema.equals(Schema.FLOAT64_SCHEMA)) {
			return Double.parseDouble(value);
		}
		if (fieldSchema.equals(org.apache.kafka.connect.data.Timestamp.SCHEMA)) {
			return Timestamp.valueOf(value);
		}
		return value;
	}
}
