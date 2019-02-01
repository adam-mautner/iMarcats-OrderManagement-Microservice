package com.imarcats.microservice.order.management.order;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.imarcats.internal.server.interfaces.order.OrderInternal;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.market.engine.order.OrderCancelActionRequestor;
import com.imarcats.market.engine.order.OrderSubmitActionRequestor;

@Service
public class OrderActionRequestor implements OrderSubmitActionRequestor, OrderCancelActionRequestor {
	
	@Autowired
	private KafkaTemplate<String, OrderActionMessage> orderActionMessageKafkaTemplate;
	
	// We have to set the topic to the one we set up for Kafka Docker - I know,
	// hardcoded topic - again :)
	public static final String IMARCATS_ORDER_QUEUE = "imarcats_order_q";
	
	public OrderActionRequestor() {

	}
	
	@Override
	public void cancelOrder(OrderInternal orderInternal_,
			String cancellationCommentLanguageKey_,
			OrderManagementContext orderManagementContext_) {
		OrderActionMessage actionMessage = new OrderActionMessage();
		actionMessage.setOrderKey(orderInternal_.getKey());
		actionMessage.setMarketCode(orderInternal_.getOrderModel().getTargetMarketCode());
		actionMessage.setOrderAction(OrderAction.Cancel);
		actionMessage.setCancellationCommentLanguageKey(cancellationCommentLanguageKey_);
		actionMessage.setVersion(orderInternal_.getOrderModel().getVersionNumber());
		
		sendMessage(actionMessage, orderInternal_.getOrderModel().getTargetMarketCode()); 
	}

	@Override
	public void submitOrder(OrderInternal orderInternal_,
			OrderManagementContext orderManagementContext_) {
		OrderActionMessage actionMessage = new OrderActionMessage();
		actionMessage.setOrderKey(orderInternal_.getKey());
		actionMessage.setMarketCode(orderInternal_.getOrderModel().getTargetMarketCode());
		actionMessage.setOrderAction(OrderAction.Submit);
		actionMessage.setVersion(orderInternal_.getOrderModel().getVersionNumber());
		
		sendMessage(actionMessage, orderInternal_.getOrderModel().getTargetMarketCode()); 
	}
	
	private void sendMessage(OrderActionMessage message, String marketCode) {
		orderActionMessageKafkaTemplate.send(IMARCATS_ORDER_QUEUE, marketCode, message);
	}


}