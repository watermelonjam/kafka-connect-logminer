package io.x.kafka.connect.logminer.sql;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LogMinerSQL {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerSQL.class);

	protected static final Charset SQL_FILE_ENCODING = Charset.forName("UTF8");
	private static final String PROPERTIES_FILE = "sql.properties";

	private static Map<Statement, String> STATEMENTS;

	static {
		STATEMENTS = new HashMap<Statement, String>();
		try {
			initializeStatements(STATEMENTS, PROPERTIES_FILE);
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	protected static void initializeStatements(Map<Statement, String> statements, String pointersFile)
			throws IOException, URISyntaxException {
		Properties pointers = new Properties();
		InputStream is = ClassLoader.getSystemResourceAsStream(pointersFile);
		pointers.load(is);

		for (String pointer : pointers.stringPropertyNames()) {
			URI resourceURI = ClassLoader.getSystemResource(pointers.getProperty(pointer)).toURI();
			LOGGER.debug("{}: {}={}", pointersFile, pointer, resourceURI.toString());

			Statement s = Statement.get(pointer);
			statements.put(s, getStatementSQL(resourceURI));
		}
	}

	protected static String getStatementSQL(URI resourceURI) throws IOException {
		String sql = null;
		if (resourceURI.getScheme().equals("file")) {
			final Path path = FileSystems.getDefault().getPath(resourceURI.getPath());
			byte[] sqlBytes = Files.readAllBytes(path);
			sql = new String(sqlBytes, SQL_FILE_ENCODING);
		} else if (resourceURI.getScheme().equals("jar")) {
			FileSystem fs = null;
			try {
				final String[] array = resourceURI.toString().split("!");
				fs = FileSystems.newFileSystem(URI.create(array[0]), Collections.emptyMap());
				final Path path = fs.getPath(array[1]);
				byte[] sqlBytes = Files.readAllBytes(path);
				sql = new String(sqlBytes, SQL_FILE_ENCODING);
			} finally {
				if (fs != null) {
					fs.close();
				}
			}
		}
		return sql;
	}

	protected String getStatement(Statement statement) {
		return STATEMENTS.get(statement);
	}

	public enum Statement {
		START_MINING("start"), STOP_MINING("stop"), CONTENTS("contents"), DICTIONARY("dictionary");

		private final String property;

		private Statement(String property) {
			this.property = property;
		}

		public String getProperty() {
			return this.property;
		}

		public static Statement get(String property) {
			switch (property) {
			case "start":
				return START_MINING;
			case "stop":
				return STOP_MINING;
			case "contents":
				return CONTENTS;
			case "dictionary":
				return DICTIONARY;
			default:
				throw new IllegalArgumentException("Invalid SQL statement property name \"" + property + "\"");
			}
		}
	};
}
