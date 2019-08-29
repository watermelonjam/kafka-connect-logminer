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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import io.extr.kafka.connect.logminer.model.Table;

public interface LogMinerDialect {
	public List<Table> getTables(Connection connection) throws SQLException;
	
	public enum Statement {
		START_MINING("start"), STOP_MINING("stop"), CONTENTS("contents"), DICTIONARY("dictionary"), CURRENT_SCN(
				"current.scn"), LATEST_SCN("latest.scn"), EARLIEST_SCN("earliest.scn"), TABLES("tables");

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
			case "current.scn":
				return CURRENT_SCN;
			case "latest.scn":
				return LATEST_SCN;
			case "earliest.scn":
				return EARLIEST_SCN;
			case "tables":
				return TABLES;
			default:
				throw new IllegalArgumentException("Invalid SQL statement property name \"" + property + "\"");
			}
		}
	};
}
