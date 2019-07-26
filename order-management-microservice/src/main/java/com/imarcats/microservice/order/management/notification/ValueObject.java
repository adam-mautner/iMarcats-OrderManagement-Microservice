package com.imarcats.microservice.order.management.notification;

public class ValueObject {
	private String value;

	public ValueObject() {
		super();
	}

	public ValueObject(String value) {
		super();
		this.value = value;
	}

	public ValueObject(Integer value) {
		super();
		this.value = value != null ? value.toString() : null;
	}
	
	public ValueObject(Long value) {
		super();
		this.value = value != null ? value.toString() : null;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
}
