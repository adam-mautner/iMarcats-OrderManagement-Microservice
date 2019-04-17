package com.imarcats.microservice.order.management.order;

public class DeleteSubmittedOrderMessage {

	private Long orderKey;

	public Long getOrderKey() {
		return orderKey;
	}

	public void setOrderKey(Long orderKey) {
		this.orderKey = orderKey;
	}
}
