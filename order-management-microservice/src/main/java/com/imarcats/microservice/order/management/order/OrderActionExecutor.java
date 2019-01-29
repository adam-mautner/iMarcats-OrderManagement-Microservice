package com.imarcats.microservice.order.management.order;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.imarcats.internal.server.infrastructure.datastore.MarketDatastore;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.market.engine.matching.OrderCancelActionExecutor;
import com.imarcats.market.engine.matching.OrderSubmitActionExecutor;

@Component
public class OrderActionExecutor {

	private OrderSubmitActionExecutor orderSubmitActionExecutor;
	private OrderCancelActionExecutor orderCancelActionExecutor;
	
	@Autowired
	@Qualifier("MarketDatastoreImpl")
	protected MarketDatastore marketDatastore;
	
	@Autowired
	@Qualifier("OrderDatastoreImpl")
	protected OrderDatastore orderDatastore;
	
	@Autowired
	protected OrderManagementContext orderManagementContext;
	
	@PostConstruct
	public void postCreate() {
		orderSubmitActionExecutor = new OrderSubmitActionExecutor(marketDatastore, orderDatastore);
		orderCancelActionExecutor = new OrderCancelActionExecutor(marketDatastore, orderDatastore);
	}
	
	@KafkaListener(topicPartitions = @TopicPartition(topic = OrderActionRequestor.IMARCATS_ORDER_QUEUE, partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0") }))
	@Transactional
	public void listenToParition(@Payload OrderActionMessage message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		if (message.getOrderAction() == OrderAction.Submit) {
			orderSubmitActionExecutor.submitOrder(message.getOrderKey(), orderManagementContext);
		} else if (message.getOrderAction() == OrderAction.Cancel) {
			orderCancelActionExecutor.cancelOrder(message.getOrderKey(), message.getCancellationCommentLanguageKey(), orderManagementContext);
		}
	}
}
