package com.imarcats.microservice.order.management.order;

import com.imarcats.interfaces.client.v100.dto.OrderDto;

public class CreateSubmittedOrderMessage {

	private OrderDto order;

	public OrderDto getOrder() {
		return order;
	}

	public void setOrder(OrderDto order) {
		this.order = order;
	}
	
}
