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

package io.extr.kafka.connect.logminer.model;

import java.util.Objects;
import java.util.Set;

public class Table implements Comparable<Table> {
	private final String databaseName;
	private final String ownerName;
	private final String tableName;
	private final int hash;

	private Long eventCount;

	public Table(String databaseName, String ownerName, String tableName, Long eventCount) {
		this.databaseName = databaseName == null || databaseName.isEmpty() ? null : databaseName;
		this.ownerName = ownerName == null || ownerName.isEmpty() ? null : ownerName;
		this.tableName = tableName == null || tableName.isEmpty() ? null : tableName;

		this.hash = Objects.hash(databaseName, ownerName, tableName);

		this.eventCount = eventCount == null ? 0L : eventCount;
	}

	public Table(String databaseName, String ownerName, String tableName) {
		this(databaseName, ownerName, tableName, null);
	}

	public static Table fromQName(String name) {
		if (name == null)
			return null;
		
		String[] nameElements = name.split("\\.");
		return new Table(nameElements[0], nameElements[1], nameElements[2]);
	}

	public String getOwnerName() {
		return ownerName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getQName() {
		return String.format("%s.%s.%s", databaseName, ownerName, tableName);
	}

	public Long getEventCount() {
		return eventCount;
	}

	public void setEventCount(Long eventCount) {
		this.eventCount = eventCount;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Table other = (Table) obj;
		return Objects.equals(databaseName, other.databaseName) && Objects.equals(ownerName, other.ownerName)
				&& Objects.equals(tableName, other.tableName);
	}

	@Override
	public String toString() {
		return "TableId [databaseName=" + databaseName + ", ownerName=" + ownerName + ", tableName=" + tableName
				+ ", eventCount=" + eventCount + "]";
	}

	@Override
	public int compareTo(Table t) {
		return eventCount.compareTo(t.getEventCount());
	}

	public boolean matches(Set<String> regexes) {
		for (String regex : regexes) {
			if (getQName().matches(regex))
				return true;
		}
		return false;
	}
}
