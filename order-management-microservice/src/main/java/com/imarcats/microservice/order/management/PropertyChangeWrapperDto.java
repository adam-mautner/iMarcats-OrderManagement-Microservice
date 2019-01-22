package com.imarcats.microservice.order.management;

import com.imarcats.interfaces.client.v100.dto.types.PropertyChangeDto;

public class PropertyChangeWrapperDto {

	private PropertyChangeDto[] propertyChanges;

	public PropertyChangeDto[] getPropertyChanges() {
		return propertyChanges;
	}

	public void setPropertyChanges(PropertyChangeDto[] propertyChanges) {
		this.propertyChanges = propertyChanges;
	}
	
}
