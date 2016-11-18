package org.learningstorm.lambda.model;

import java.util.UUID;

public class BaseModel {
	private UUID id = null;
	private UUID dataId = null;
	private long timestamp = 0;
	private boolean deleted = false;
	
	public String toString() {
		return String.format("id: %s\n"
				+ "dataId: %s\n"
				+ "timestamp: %s\n"
				+ "deleted: %b",
				id, dataId, timestamp, deleted);
	}

	public UUID getId() {
		return id;
	}
	
	public void setId(UUID id) {
		this.id = id;
	}
	
	public UUID getDataId() {
		return dataId;
	}
	
	public void setDataId(UUID dataId) {
		this.dataId = dataId;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public boolean isDeleted() {
		return deleted;
	}
	
	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
}
