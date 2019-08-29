package io.extr.kafka.connect.logminer.model;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import io.extr.kafka.connect.logminer.LogMinerSourceConnectorConstants;

public class LogMinerEvent {
	private final Schema schema;
	private final Struct struct;

	public LogMinerEvent(Schema schema, Struct struct) {
		this.schema = schema;
		this.struct = struct;
	}

	public Schema getSchema() {
		return this.schema;
	}

	public Struct getStruct() {
		return this.struct;
	}

	public Map<String, Object> getOffset() {
		return new Offset(struct.getInt64(LogMinerSourceConnectorConstants.FIELD_SCN),
				struct.getInt64(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN),
				struct.getString(LogMinerSourceConnectorConstants.FIELD_ROW_ID),
				Timestamp.toLogical(struct.schema().field(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP).schema(),
						(Long) struct.getInt64(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP))).toMap();
	}

	public Map<String, String> getPartition() {
		return Collections.singletonMap(LogMinerSourceConnectorConstants.TABLE_NAME_KEY, schema.name());
	}

	@Override
	public String toString() {
		return "LogMinerEvent [schema=" + schema + ", struct=" + struct + "]";
	}
}
