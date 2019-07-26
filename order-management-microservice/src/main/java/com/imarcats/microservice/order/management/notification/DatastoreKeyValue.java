package com.imarcats.microservice.order.management.notification;

import com.imarcats.interfaces.client.v100.dto.types.DatastoreKeyDto;

public class DatastoreKeyValue {

	private String codeKey;
	private Long idKey;
	private String key;
	public String getCodeKey() {
		return codeKey;
	}
	public void setCodeKey(String codeKey) {
		this.codeKey = codeKey;
	}
	public Long getIdKey() {
		return idKey;
	}
	public void setIdKey(Long idKey) {
		this.idKey = idKey;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	
	public DatastoreKeyDto createDto() {
		if (idKey != null) {
			return new DatastoreKeyDto(idKey);
		} else {
			return new DatastoreKeyDto(codeKey);
		}
	}
}
