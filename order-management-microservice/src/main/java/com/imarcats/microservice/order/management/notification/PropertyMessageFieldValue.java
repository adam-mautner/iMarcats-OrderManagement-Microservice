package com.imarcats.microservice.order.management.notification;

import java.util.Arrays;
import java.util.Date;

import com.imarcats.interfaces.client.v100.dto.helpers.MarketStateWrapperDto;
import com.imarcats.interfaces.client.v100.dto.helpers.OrderStateWrapperDto;
import com.imarcats.interfaces.client.v100.dto.types.DatePropertyDto;
import com.imarcats.interfaces.client.v100.dto.types.IntPropertyDto;
import com.imarcats.interfaces.client.v100.dto.types.MarketState;
import com.imarcats.interfaces.client.v100.dto.types.ObjectPropertyDto;
import com.imarcats.interfaces.client.v100.dto.types.OrderState;
import com.imarcats.interfaces.client.v100.dto.types.PropertyDto;
import com.imarcats.interfaces.client.v100.dto.types.StringPropertyDto;

public class PropertyMessageFieldValue {
	private String name;
	private ValueObject value;
	private String propertyType;
	private String unit;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public ValueObject getValue() {
		return value;
	}
	public void setValue(ValueObject value) {
		this.value = value;
	}
	public String getPropertyType() {
		return propertyType;
	}
	public void setPropertyType(String propertyType) {
		this.propertyType = propertyType;
	}
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	public PropertyDto creataDto() {
		// TODO: This is hack, fix it :)
		// TODO: Handle non-Object properties !!! 
		if ("Object".equals(propertyType)) {
			ObjectPropertyDto objDto = new ObjectPropertyDto();
			objDto.setName(name);
			// TODO: We need to add an object type here 
			if (Arrays.asList(MarketState.values()).stream().filter(v -> value.getValue().equals(v.toString())).findAny().isPresent()) {				
				objDto.setValue(new MarketStateWrapperDto(MarketState.valueOf(value.getValue())));
			} else if (Arrays.asList(OrderState.values()).stream().filter(v -> value.getValue().equals(v.toString())).findAny().isPresent()) {
				objDto.setValue(new OrderStateWrapperDto(OrderState.valueOf(value.getValue())));
			}
			
			return objDto;
		} else if ("String".equals(propertyType)) {
			StringPropertyDto objDto = new StringPropertyDto();
			objDto.setName(name);
			if (value != null) {				
				objDto.setValue(value.getValue());
			}
			
			return objDto;
		} else if ("Date".equals(propertyType)) {
			DatePropertyDto objDto = new DatePropertyDto();
			objDto.setName(name);
			if (value != null) {				
				objDto.setValue(value.getValue() != null ? new Date(Long.parseLong(value.getValue())): null);
			}
			
			return objDto;
		} else if ("Int".equals(propertyType)) {
			IntPropertyDto objDto = new IntPropertyDto();
			objDto.setName(name);
			if (value != null) {				
				objDto.setValue(value.getValue() != null ? Integer.parseInt(value.getValue()): null);
			}
			
			return objDto;
		}
		return null;
	}
	
}
