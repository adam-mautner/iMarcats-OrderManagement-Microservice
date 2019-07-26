package com.imarcats.microservice.order.management.notification;

import com.imarcats.interfaces.client.v100.dto.types.ChangeAction;
import com.imarcats.interfaces.client.v100.dto.types.PropertyChangeDto;
import com.imarcats.interfaces.client.v100.dto.types.PropertyListValueChangeDto;
import com.imarcats.interfaces.client.v100.dto.types.PropertyValueChangeDto;

public class PropertyValueChangeMessageField {
	private ChangeAction changeAction;
	private String propertyListName; 
	private PropertyMessageFieldValue property;
	public ChangeAction getChangeAction() {
		return changeAction;
	}
	public void setChangeAction(ChangeAction changeAction) {
		this.changeAction = changeAction;
	}
	public String getPropertyListName() {
		return propertyListName;
	}
	public void setPropertyListName(String propertyListName) {
		this.propertyListName = propertyListName;
	}
	public PropertyMessageFieldValue getProperty() {
		return property;
	}
	public void setProperty(PropertyMessageFieldValue property) {
		this.property = property;
	}
	
	public PropertyChangeDto createDto() {
		if (propertyListName != null) {
			PropertyListValueChangeDto listValueChangeDto = new PropertyListValueChangeDto();
			listValueChangeDto.setPropertyListName(propertyListName);
			listValueChangeDto.setChangeAction(changeAction);
			listValueChangeDto.setProperty(property != null ? property.creataDto() : null);
			return listValueChangeDto;
		} else {
			PropertyValueChangeDto changeDto = new PropertyValueChangeDto();
			changeDto.setProperty(property.creataDto());
			return changeDto;
		}
	}
	
}
