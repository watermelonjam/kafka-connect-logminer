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

import java.util.Comparator;
import java.util.Objects;

public class TableId implements Comparable<TableId> {
	private final String containerName;
	private final String schemaName;
	private final String tableName;
	private final int hash;

	private Long eventCount;

	public TableId(String containerName, String schemaName, String tableName, Long eventCount) {
		this.containerName = containerName == null || containerName.isEmpty() ? null : containerName;
		this.schemaName = schemaName == null || schemaName.isEmpty() ? null : schemaName;
		this.tableName = tableName == null || tableName.isEmpty() ? null : tableName;
		this.hash = Objects.hash(containerName, schemaName, tableName);
		this.eventCount = eventCount == null ? 0L : eventCount;
	}

	public TableId(String containerName, String schemaName, String tableName) {
		this(containerName, schemaName, tableName, null);
	}

	public String getContainerName() {
		return containerName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
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
		TableId other = (TableId) obj;
		return Objects.equals(containerName, other.containerName) && Objects.equals(schemaName, other.schemaName)
				&& Objects.equals(tableName, other.tableName);
	}

	@Override
	public String toString() {
		return "TableId [containerName=" + containerName + ", schemaName=" + schemaName + ", tableName=" + tableName
				+ ", eventCount=" + eventCount + "]";
	}

	@Override
	public int compareTo(TableId o) {
		if (o == this) {
			return 0;
		}
		int diff = this.tableName.compareTo(o.tableName);
		if (diff != 0) {
			return diff;
		}
		if (this.schemaName == null) {
			if (o.schemaName != null) {
				return -1;
			}
		} else {
			if (o.schemaName == null) {
				return 1;
			}
			diff = this.schemaName.compareTo(o.schemaName);
			if (diff != 0) {
				return diff;
			}
		}
		if (this.containerName == null) {
			if (o.containerName != null) {
				return -1;
			}
		} else {
			if (o.containerName == null) {
				return 1;
			}
			diff = this.containerName.compareTo(o.containerName);
			if (diff != 0) {
				return diff;
			}
		}
		return 0;
	}

	public static class TableIdComparator implements Comparator<TableId> {
		@Override
		public int compare(TableId t1, TableId t2) {
			return t1.getEventCount().compareTo(t2.getEventCount());
		}

	}
}
