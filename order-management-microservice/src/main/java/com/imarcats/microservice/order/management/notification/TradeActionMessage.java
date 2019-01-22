package com.imarcats.microservice.order.management.notification;

import com.imarcats.interfaces.client.v100.dto.types.OrderSide;

public class TradeActionMessage {
	/**
	 * Trade transaction ID
	 */
	private long transactionID;
	
	/**
	 * Market code 
	 */
	private String marketCode;
	
	/**
	 * Side (Buy/Sell)
	 */
	private OrderSide side;

	public long getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(long transactionID_) {
		transactionID = transactionID_;
	}

	public String getMarketCode() {
		return marketCode;
	}

	public void setMarketCode(String marketCode_) {
		marketCode = marketCode_;
	}

	public OrderSide getSide() {
		return side;
	}

	public void setSide(OrderSide side_) {
		side = side_;
	}
}
