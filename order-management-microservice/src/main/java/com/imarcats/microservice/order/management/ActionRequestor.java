package com.imarcats.microservice.order.management;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.imarcats.interfaces.client.v100.dto.MarketDto;
import com.imarcats.interfaces.client.v100.dto.OrderDto;
import com.imarcats.interfaces.client.v100.dto.types.TimeOfDayDto;
import com.imarcats.interfaces.server.v100.dto.mapping.OrderDtoMapping;
import com.imarcats.internal.server.interfaces.order.OrderInternal;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.market.engine.order.OrderCancelActionRequestor;
import com.imarcats.market.engine.order.OrderSubmitActionRequestor;
import com.imarcats.microservice.order.management.market.CallMarketMessage;
import com.imarcats.microservice.order.management.market.CloseMarketMessage;
import com.imarcats.microservice.order.management.market.CreateActiveMarketMessage;
import com.imarcats.microservice.order.management.market.DeleteActiveMarketMessage;
import com.imarcats.microservice.order.management.market.OpenMarketMessage;
import com.imarcats.microservice.order.management.order.CreateSubmittedOrderMessage;
import com.imarcats.microservice.order.management.order.DeleteSubmittedOrderMessage;
import com.imarcats.microservice.order.management.order.OrderAction;
import com.imarcats.microservice.order.management.order.OrderActionMessage;

@Service
public class ActionRequestor implements OrderSubmitActionRequestor, OrderCancelActionRequestor {
	
	@Autowired
	private KafkaTemplate<String, UpdateMessage> orderActionMessageKafkaTemplate;
	
	// We have to set the topic to the one we set up for Kafka Docker - I know,
	// hardcoded topic - again :)
	public static final String IMARCATS_ORDER_QUEUE = "imarcats_order_q";
	
	public ActionRequestor() {

	}
	
	@Override
	public void cancelOrder(OrderInternal orderInternal_,
			String cancellationCommentLanguageKey_,
			OrderManagementContext orderManagementContext_) {
		// cancel order 
		OrderActionMessage actionMessage = new OrderActionMessage();
		actionMessage.setOrderKey(orderInternal_.getKey());
		actionMessage.setMarketCode(orderInternal_.getOrderModel().getTargetMarketCode());
		actionMessage.setOrderAction(OrderAction.Cancel);
		actionMessage.setCancellationCommentLanguageKey(cancellationCommentLanguageKey_);
		actionMessage.setVersion(orderInternal_.getOrderModel().getVersionNumber());
		
		UpdateMessage message = new UpdateMessage();
		message.setOrderActionMessage(actionMessage);
		
		sendMessage(message, orderInternal_.getOrderModel().getTargetMarketCode()); 
		
		// delete the cancelled order from order matching 
		message = new UpdateMessage();
		DeleteSubmittedOrderMessage deleteSubmittedOrderMessage = new DeleteSubmittedOrderMessage();
		deleteSubmittedOrderMessage.setOrderKey(orderInternal_.getOrderModel().getKey());
		message.setDeleteSubmittedOrderMessage(deleteSubmittedOrderMessage);
		
		sendMessage(message, orderInternal_.getOrderModel().getTargetMarketCode()); 
		
	}

	@Override
	public void submitOrder(OrderInternal orderInternal_,
			OrderManagementContext orderManagementContext_) {
		// create order in order matching before submitting it 
		UpdateMessage message = new UpdateMessage();
		CreateSubmittedOrderMessage createSubmittedOrderMessage = new CreateSubmittedOrderMessage();
		OrderDto orderDto = OrderDtoMapping.INSTANCE.toDto(orderInternal_.getOrderModel());
		
		// TODO: This is a hack for now 
		orderDto.setLastUpdateTimestamp(new Date());
		
		createSubmittedOrderMessage.setOrder(orderDto);
		message.setCreateSubmittedOrderMessage(createSubmittedOrderMessage);
		
		sendMessage(message, orderInternal_.getOrderModel().getTargetMarketCode()); 
		
		// submit order 
		OrderActionMessage actionMessage = new OrderActionMessage();
		actionMessage.setOrderKey(orderInternal_.getKey());
		actionMessage.setMarketCode(orderInternal_.getOrderModel().getTargetMarketCode());
		actionMessage.setOrderAction(OrderAction.Submit);
		actionMessage.setVersion(orderInternal_.getOrderModel().getVersionNumber());
		
		message = new UpdateMessage();
		message.setOrderActionMessage(actionMessage);
		
		sendMessage(message, orderInternal_.getOrderModel().getTargetMarketCode()); 
	}
	
	public void createActiveMarket(MarketDto market) {
		UpdateMessage message = new UpdateMessage();
		CreateActiveMarketMessage createActiveMarketMessage = new CreateActiveMarketMessage();
		createActiveMarketMessage.setMarket(market);
		message.setCreateActiveMarketMessage(createActiveMarketMessage);
		
		sendMessage(message, market.getMarketCode()); 
	}

	public void deleteActiveMarket(String marketCode) {
		UpdateMessage message = new UpdateMessage();
		DeleteActiveMarketMessage deleteActiveMarketMessage = new DeleteActiveMarketMessage();
		deleteActiveMarketMessage.setMarketCode(marketCode);
		message.setDeleteActiveMarketMessage(deleteActiveMarketMessage);
		
		sendMessage(message, marketCode);
	}
	
	public void openMarket(String marketCode) {
		UpdateMessage message = new UpdateMessage();
		OpenMarketMessage openMarketMessage = new OpenMarketMessage();
		openMarketMessage.setMarketCode(marketCode);
		message.setOpenMarketMessage(openMarketMessage);
		
		sendMessage(message, marketCode);
	}

	public void closeMarket(String marketCode) {
		UpdateMessage message = new UpdateMessage();
		CloseMarketMessage closeMarketMessage = new CloseMarketMessage();
		closeMarketMessage.setMarketCode(marketCode);
		message.setCloseMarketMessage(closeMarketMessage);
		
		sendMessage(message, marketCode);
	}

	public void callMarket(String marketCode, Date nextCallDate, TimeOfDayDto nextCallTime) {
		UpdateMessage message = new UpdateMessage();
		CallMarketMessage callMarketMessage = new CallMarketMessage();
		callMarketMessage.setMarketCode(marketCode);
		callMarketMessage.setNextCallDate(nextCallDate);
		callMarketMessage.setNextCallTime(nextCallTime); 
		message.setCallMarketMessage(callMarketMessage);
		
		sendMessage(message, marketCode);
	}
	
	private void sendMessage(UpdateMessage message, String marketCode) {
		orderActionMessageKafkaTemplate.send(IMARCATS_ORDER_QUEUE, marketCode, message);
	}
}