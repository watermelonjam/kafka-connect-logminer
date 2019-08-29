package io.extr.kafka.connect.logminer.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.extr.kafka.connect.logminer.LogMinerSourceConnectorConstants;

public class Offset {
	private Long systemChangeNumber;
	private Long commitSystemChangeNumber;
	private String rowId;
	private Date timestamp;

	public static final Offset DEFAULT_OFFSET = new Offset(0L, 0L, null, null);
	
	public Offset(Long systemChangeNumber, Long commitSystemChangeNumber, String rowId, Date timestamp) {
		super();
		this.systemChangeNumber = systemChangeNumber;
		this.commitSystemChangeNumber = commitSystemChangeNumber;
		this.rowId = rowId;
		this.timestamp = timestamp;
	}

	public Long getSystemChangeNumber() {
		return systemChangeNumber;
	}

	public void setSystemChangeNumber(Long systemChangeNumber) {
		this.systemChangeNumber = systemChangeNumber;
	}

	public Long getCommitSystemChangeNumber() {
		return commitSystemChangeNumber;
	}

	public void setCommitSystemChangeNumber(Long commitSystemChangeNumber) {
		this.commitSystemChangeNumber = commitSystemChangeNumber;
	}

	public String getRowId() {
		return rowId;
	}

	public void setRowId(String rowId) {
		this.rowId = rowId;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();

		map.put(LogMinerSourceConnectorConstants.FIELD_SCN, this.systemChangeNumber);
		map.put(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN, this.commitSystemChangeNumber);
		map.put(LogMinerSourceConnectorConstants.FIELD_ROW_ID, this.rowId);
		map.put(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP, this.timestamp);

		return map;
	}

	public static Offset fromMap(Map<String, Object> map) {
		if (map == null || map.keySet().size() == 0) {
			return DEFAULT_OFFSET;
		}

		Long mapSystemChangeNumber = map.get(LogMinerSourceConnectorConstants.FIELD_SCN) == null ? null
				: (Long) map.get(LogMinerSourceConnectorConstants.FIELD_SCN);
		Long mapCommitSystemChangeNumber = map.get(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN) == null ? null
				: (Long) map.get(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN);
		String mapRowId = map.get(LogMinerSourceConnectorConstants.FIELD_ROW_ID) == null ? null
				: (String) map.get(LogMinerSourceConnectorConstants.FIELD_ROW_ID);
		Date mapTimestamp = map.get(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP) == null ? null
				: (Date) map.get(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP);

		return new Offset(mapSystemChangeNumber, mapCommitSystemChangeNumber, mapRowId, mapTimestamp);
	}
}
