package com.imarcats.microservice.order.management.order;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.imarcats.internal.server.infrastructure.datastore.MarketDatastore;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.interfaces.order.OrderInternal;
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
	public void listenToOrderActionQueueParition(@Payload OrderActionMessage message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header(KafkaHeaders.OFFSET) int offset) {

		// this is needed, because Kafka message arrives faster than the actual change is committed to the datastore (because of the lack of transactions) 
		// TODO: Remove it once the transactions are correct
		try {
			Thread.sleep(500);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			if (message.getOrderAction() == OrderAction.Submit) {
				if (!checkStale(message)) {
					orderSubmitActionExecutor.submitOrder(message.getOrderKey(), orderManagementContext);
				}
			} else if (message.getOrderAction() == OrderAction.Cancel) {
				if (!checkStale(message)) {
					orderCancelActionExecutor.cancelOrder(message.getOrderKey(),
							message.getCancellationCommentLanguageKey(), orderManagementContext);
				}
			} else {
				// TODO: Add proper logging
				System.out.println("Unkown action: " + message.getOrderAction());
			}
		} catch (Exception e) {
			// TODO: handle exception better
			// TODO: Add proper logging
			System.out.println("Error processing order action: " + message.getOrderAction() + " for order: "
					+ message.getOrderKey() + ", error: " + e);
		}
	}

	private boolean checkStale(OrderActionMessage message) {
		// we are mostly concerned about resubmitting stale orders. For now, we can be easier on cancellation (we may revisit this later). 
		OrderInternal order = orderDatastore.findOrderBy(message.getOrderKey());
		if (order == null) {
			// non-existent order
			// TODO: Add proper logging
			System.out.println("Non-existent order: " + message.getOrderKey());
			return true;
		}
		// this serves 2 purposes: 
		// - provides idempotence for message processing 
		// - ignores stale messages on restart, but this is not too efficient (better way to store initial offset in DB or in Zookeeper and set it to initial offset above)
		// Note: We have to check the message version + 1, because it was created before the order was committed (because of lack of transactions)
		// TODO: Remove it once the transactions are correct 
		if (order.getOrderModel().getVersionNumber() > message.getVersion() + 1) {
			// stale order
			// TODO: Add proper logging
			System.out.println("Stale order: " + message.getOrderKey() + ", message version: " + message.getVersion() + ", order version: " + order.getOrderModel().getVersionNumber() + 1);
			return true;
		}

		return false;
	}

}
