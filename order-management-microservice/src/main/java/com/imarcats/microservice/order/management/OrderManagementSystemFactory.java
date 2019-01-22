package com.imarcats.microservice.order.management;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.imarcats.internal.server.infrastructure.datastore.MarketDatastore;
import com.imarcats.internal.server.infrastructure.datastore.MatchedTradeDatastore;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.interfaces.order.OrderInternal;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.market.engine.matching.OrderCancelActionExecutor;
import com.imarcats.market.engine.matching.OrderSubmitActionExecutor;
import com.imarcats.market.engine.order.OrderCancelActionRequestor;
import com.imarcats.market.engine.order.OrderManagementSystem;
import com.imarcats.market.engine.order.OrderSubmitActionRequestor;

@Configuration
public class OrderManagementSystemFactory {

	@Autowired
	@Qualifier("MarketDatastoreImpl")
	protected MarketDatastore marketDatastore;
	
	@Autowired
	@Qualifier("OrderDatastoreImpl")
	protected OrderDatastore orderDatastore;
	
	@Autowired
	@Qualifier("MatchedTradeDatastoreImpl")
	protected MatchedTradeDatastore tradeDatastore;
	
	@Bean
	public OrderManagementSystem createOrderManagementSystem() {
		MockOrderActionRequestor mockOrderActionRequestor = new MockOrderActionRequestor();
		return new OrderManagementSystem(marketDatastore, orderDatastore, tradeDatastore, mockOrderActionRequestor, mockOrderActionRequestor);
	}
	
	private class MockOrderActionRequestor implements OrderSubmitActionRequestor, OrderCancelActionRequestor {
		
		private OrderSubmitActionExecutor _orderSubmitActionExecutor;
		private OrderCancelActionExecutor _orderCancelActionExecutor;

		public MockOrderActionRequestor() {
			super();
			_orderSubmitActionExecutor = new OrderSubmitActionExecutor(marketDatastore, orderDatastore);
			_orderCancelActionExecutor = new OrderCancelActionExecutor(marketDatastore, orderDatastore);
		}
		
		@Override
		public void cancelOrder(OrderInternal orderInternal_,
				String cancellationCommentLanguageKey_,
				OrderManagementContext orderManagementContext_) {
			// TODO: Separate this action with Kafka communication 
			_orderCancelActionExecutor.cancelOrder(orderInternal_.getKey(), cancellationCommentLanguageKey_, orderManagementContext_);
		}

		@Override
		public void submitOrder(OrderInternal orderInternal_,
				OrderManagementContext orderManagementContext_) {
			// TODO: Separate this action with Kafka communication 
			_orderSubmitActionExecutor.submitOrder(orderInternal_.getKey(), orderManagementContext_);
		}

	}
}
