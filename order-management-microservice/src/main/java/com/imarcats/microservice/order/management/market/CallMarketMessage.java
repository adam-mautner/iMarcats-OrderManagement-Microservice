package com.imarcats.microservice.order.management.market;

import java.util.Date;

import com.imarcats.interfaces.client.v100.dto.types.TimeOfDayDto;

public class CallMarketMessage {
	private String marketCode;
	private Date nextCallDate; 
	private TimeOfDayDto nextCallTime;

	public String getMarketCode() {
		return marketCode;
	}

	public void setMarketCode(String marketCode) {
		this.marketCode = marketCode;
	}

	public Date getNextCallDate() {
		return nextCallDate;
	}

	public void setNextCallDate(Date nextCallDate) {
		this.nextCallDate = nextCallDate;
	}

	public TimeOfDayDto getNextCallTime() {
		return nextCallTime;
	}

	public void setNextCallTime(TimeOfDayDto nextCallTime) {
		this.nextCallTime = nextCallTime;
	}
	
}
