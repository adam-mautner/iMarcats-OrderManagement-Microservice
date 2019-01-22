package com.imarcats.microservice.order.management;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.imarcats.interfaces.client.v100.dto.MatchedTradeDto;
import com.imarcats.interfaces.client.v100.dto.types.DatastoreKeyDto;
import com.imarcats.interfaces.client.v100.dto.types.PropertyChangeDto;
import com.imarcats.interfaces.client.v100.notification.ChangeOrigin;
import com.imarcats.interfaces.client.v100.notification.MarketDataChange;
import com.imarcats.interfaces.client.v100.notification.ObjectVersion;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;
import com.imarcats.internal.server.infrastructure.datastore.MarketDatastore;
import com.imarcats.internal.server.infrastructure.datastore.MatchedTradeDatastore;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSession;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSessionImpl;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSource;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSourceImpl;
import com.imarcats.internal.server.infrastructure.notification.NotificationBroker;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeBroker;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeBrokerImpl;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeSession;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeSessionImpl;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationBroker;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationBrokerImpl;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationSession;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationSessionImpl;
import com.imarcats.internal.server.interfaces.order.OrderInternal;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.internal.server.interfaces.order.OrderManagementContextImpl;
import com.imarcats.market.engine.matching.OrderCancelActionExecutor;
import com.imarcats.market.engine.matching.OrderSubmitActionExecutor;
import com.imarcats.market.engine.order.OrderCancelActionRequestor;
import com.imarcats.market.engine.order.OrderManagementSystem;
import com.imarcats.market.engine.order.OrderSubmitActionRequestor;
import com.imarcats.microservice.order.management.notification.KafkaMessageBroker;
import com.imarcats.microservice.order.management.notification.NotificationBrokerImpl;

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
	
	@Autowired
	protected KafkaMessageBroker kafkaMessageBroker;
	
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
	
	@Bean
	public OrderManagementContext createOrderManagementContext() {
		NotificationBroker broker = new NotificationBrokerImpl(kafkaMessageBroker);
		MarketDataSource marketDataSource = new MarketDataSourceImpl(broker); 
		MarketDataSession marketDataSession = new MarketDataSessionImpl(marketDataSource);
		PropertyChangeBroker propertyChangeBroker = new PropertyChangeBrokerImpl(broker);  
		PropertyChangeSession propertyChangeSession = new PropertyChangeSessionImpl(propertyChangeBroker);
		TradeNotificationBroker tradeNotificationBroker = new TradeNotificationBrokerImpl(broker);  
		TradeNotificationSession tradeNotificationSession = new TradeNotificationSessionImpl(tradeNotificationBroker);
		
		OrderManagementContext context = new OrderManagementContextImpl(
				marketDataSession,
				propertyChangeSession,
				tradeNotificationSession);
		return context;
	}
	
	private class MockMarketDataSessionImpl extends MarketDataSessionImpl {
		private final List<MarketDataChange> _dataChanges = new ArrayList<MarketDataChange>();

		public MockMarketDataSessionImpl(MarketDataSource marketDataSource_) {
			super(marketDataSource_);
		}
		
		protected void notify(MarketDataChange change) {
			_dataChanges.add(change);
		}
		
		public void commit() {
			// clear the market data changes already sent to prevent duplications
			_dataChanges.clear();
		}
		
		public void rollback() {
			_dataChanges.clear();
		}

		public MarketDataChange[] getMarketDataChanges() {
			return _dataChanges.toArray(new MarketDataChange[_dataChanges.size()]);
		}
	}
	
	private class MockPropertyChangeSessionImpl extends PropertyChangeSessionImpl {

		private final List<PropertyChanges> _propertyChanges = new ArrayList<PropertyChanges>();

		public MockPropertyChangeSessionImpl(PropertyChangeBroker propertyChangeBroker_) {
			super(propertyChangeBroker_);
		}

		public void commit() {
			// _propertyChangeBroker.notifyListeners(getPropertyChanges());
			// clear the property changes already sent to prevent duplications
			_propertyChanges.clear();
		}
		
		public void rollback() {
			_propertyChanges.clear();
		}
		
		public PropertyChanges[] getPropertyChanges() {
			return _propertyChanges.toArray(new PropertyChanges[_propertyChanges.size()]);
		}

		protected void notify(Class objectClass_, Date changeTimestamp_,
				ObjectVersion version_, String owner_, ChangeOrigin changeOrigin_,
				PropertyChangeDto[] propertyChangeDtos,
				DatastoreKeyDto parentKeyDto, DatastoreKeyDto objectKeyDto) {
			_propertyChanges.add(new PropertyChanges(objectKeyDto, objectClass_, parentKeyDto, 
					propertyChangeDtos, changeTimestamp_, version_, owner_, changeOrigin_));
		}
	}
	
	private class MockTradeNotificationSessionImpl extends TradeNotificationSessionImpl {
		private final List<MatchedTradeDto> _matchedTrades = new ArrayList<MatchedTradeDto>();
		
		public MockTradeNotificationSessionImpl(
				TradeNotificationBroker tradeNotificationBroker_) {
			super(tradeNotificationBroker_);
		}

		protected void notify(MatchedTradeDto clonedMatchedTrade) {
			_matchedTrades.add(clonedMatchedTrade);
		}

		public MatchedTradeDto[] getTrades() {
			return _matchedTrades.toArray(new MatchedTradeDto[_matchedTrades.size()]);
		}
		
		public void commit() {
			// clear changes already sent to prevent duplications
			_matchedTrades.clear();
		}
		
		public void rollback() {
			// TODO Auto-generated method stub	
		}
	}
}
