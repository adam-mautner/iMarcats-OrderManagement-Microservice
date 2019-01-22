package com.imarcats.microservice.order.management;

import java.util.Date;

public class SubmitCancelDto {
	private String orderKey; 
	private Date lastUpdateTimestamp;
	
	public String getOrderKey() {
		return orderKey;
	}
	public void setOrderKey(String orderKey) {
		this.orderKey = orderKey;
	}
	public Date getLastUpdateTimestamp() {
		return lastUpdateTimestamp;
	}
	public void setLastUpdateTimestamp(Date lastUpdateTimestamp) {
		this.lastUpdateTimestamp = lastUpdateTimestamp;
	}
	
}
