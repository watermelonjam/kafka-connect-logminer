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

package io.extr.kafka.connect.logminer.dialect;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseLogMinerDialect implements LogMinerDialect {
	private static final Logger LOGGER = LoggerFactory.getLogger(BaseLogMinerDialect.class);

	protected static final Charset SQL_FILE_ENCODING = Charset.forName("UTF8");
	private static final String PROPERTIES_FILE = "sql-base.properties";

	private static Map<Statement, String> STATEMENTS;

	static {
		STATEMENTS = new HashMap<Statement, String>();
		try {
			initializeStatements(STATEMENTS, PROPERTIES_FILE);
		} catch (Exception e) {
			LOGGER.error("Cannot initialize log miner SQL", e);
			throw new ExceptionInInitializerError(e);
		}
	}

	protected static void initializeStatements(Map<Statement, String> statements, String pointersFile)
			throws IOException, URISyntaxException {
		Properties pointers = new Properties();
		InputStream is = ClassLoader.getSystemResourceAsStream(pointersFile);
		if (is == null) {
			throw new IOException("Cannot find statement SQL file " + pointersFile);
		}
		pointers.load(is);

		for (String pointer : pointers.stringPropertyNames()) {
			String pointerLocation = pointers.getProperty(pointer);
			URL resourceURL = ClassLoader.getSystemResource(pointerLocation);
			if (resourceURL == null) {
				throw new IllegalStateException(String.format("Cannot initialize SQL resource %s from file %s", pointer, pointerLocation));
			}
			URI resourceURI = resourceURL.toURI();
			LOGGER.debug("{}: {}={}", pointersFile, pointer, resourceURI.toString());

			Statement s = Statement.get(pointer);
			statements.put(s, getStatementSQL(resourceURI));
		}
	}

	protected static String getStatementSQL(URI resourceURI) throws IOException {
		String sql = null;
		if (resourceURI.getScheme().equals("file")) {
			final Path path = Paths.get(resourceURI);
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

	public String getStatement(Statement statement) {
		return STATEMENTS.get(statement);
	}
}
