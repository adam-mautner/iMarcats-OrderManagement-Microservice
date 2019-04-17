package com.imarcats.microservice.order.management;

import com.imarcats.microservice.order.management.market.CreateActiveMarketMessage;
import com.imarcats.microservice.order.management.market.DeleteActiveMarketMessage;
import com.imarcats.microservice.order.management.order.CreateSubmittedOrderMessage;
import com.imarcats.microservice.order.management.order.DeleteSubmittedOrderMessage;
import com.imarcats.microservice.order.management.order.OrderActionMessage;

public class UpdateMessage {

	private CreateActiveMarketMessage createActiveMarketMessage;
	private DeleteActiveMarketMessage deleteActiveMarketMessage;
	
	private CreateSubmittedOrderMessage createSubmittedOrderMessage;
	private DeleteSubmittedOrderMessage deleteSubmittedOrderMessage;
	
	private OrderActionMessage orderActionMessage;
	
	public CreateActiveMarketMessage getCreateActiveMarketMessage() {
		return createActiveMarketMessage;
	}
	public void setCreateActiveMarketMessage(CreateActiveMarketMessage createActiveMarketMessage) {
		this.createActiveMarketMessage = createActiveMarketMessage;
	}
	public DeleteActiveMarketMessage getDeleteActiveMarketMessage() {
		return deleteActiveMarketMessage;
	}
	public void setDeleteActiveMarketMessage(DeleteActiveMarketMessage deleteActiveMarketMessage) {
		this.deleteActiveMarketMessage = deleteActiveMarketMessage;
	}
	public OrderActionMessage getOrderActionMessage() {
		return orderActionMessage;
	}
	public void setOrderActionMessage(OrderActionMessage orderActionMessage) {
		this.orderActionMessage = orderActionMessage;
	}
	public CreateSubmittedOrderMessage getCreateSubmittedOrderMessage() {
		return createSubmittedOrderMessage;
	}
	public void setCreateSubmittedOrderMessage(CreateSubmittedOrderMessage createSubmittedOrderMessage) {
		this.createSubmittedOrderMessage = createSubmittedOrderMessage;
	}
	public DeleteSubmittedOrderMessage getDeleteSubmittedOrderMessage() {
		return deleteSubmittedOrderMessage;
	}
	public void setDeleteSubmittedOrderMessage(DeleteSubmittedOrderMessage deleteSubmittedOrderMessage) {
		this.deleteSubmittedOrderMessage = deleteSubmittedOrderMessage;
	}
	
}
