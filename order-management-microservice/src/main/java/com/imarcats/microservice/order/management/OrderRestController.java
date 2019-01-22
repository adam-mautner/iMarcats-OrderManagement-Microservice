package com.imarcats.microservice.order.management;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.imarcats.interfaces.client.v100.dto.MatchedTradeDto;
import com.imarcats.interfaces.client.v100.dto.OrderDto;
import com.imarcats.interfaces.client.v100.dto.types.DatastoreKeyDto;
import com.imarcats.interfaces.client.v100.dto.types.MarketDataType;
import com.imarcats.interfaces.client.v100.dto.types.PagedOrderListDto;
import com.imarcats.interfaces.client.v100.dto.types.PropertyChangeDto;
import com.imarcats.interfaces.client.v100.notification.ChangeOrigin;
import com.imarcats.interfaces.client.v100.notification.MarketDataChange;
import com.imarcats.interfaces.client.v100.notification.ObjectVersion;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;
import com.imarcats.interfaces.server.v100.dto.mapping.OrderDtoMapping;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSession;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSessionImpl;
import com.imarcats.internal.server.infrastructure.marketdata.MarketDataSource;
import com.imarcats.internal.server.infrastructure.marketdata.PersistedMarketDataChangeListener;
import com.imarcats.internal.server.infrastructure.notification.NotificationBroker;
import com.imarcats.internal.server.infrastructure.notification.properties.PersistedPropertyChangeListener;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeBroker;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeSession;
import com.imarcats.internal.server.infrastructure.notification.properties.PropertyChangeSessionImpl;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationBroker;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationSession;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotificationSessionImpl;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.internal.server.interfaces.order.OrderManagementContextImpl;
import com.imarcats.market.engine.order.OrderManagementSystem;
import com.imarcats.model.types.DatastoreKey;
import com.imarcats.model.types.PagedOrderList;

@RestController
@RequestMapping("/")
public class OrderRestController {

	@Autowired
	protected OrderManagementSystem orderManagementSystem;
	
	@Autowired
	@Qualifier("OrderDatastoreImpl")
	protected OrderDatastore orderDatastore;
	
	@RequestMapping(value = "/orders/**/{marketCode}", method = RequestMethod.GET, produces = { "application/JSON" })
	public PagedOrderListDto getOrdersByMarket(@PathVariable String marketCode, 
			@RequestParam("userId") Optional<String> userId,
			@RequestParam("cursorString") Optional<String> cursorString,
			@RequestParam("numberOfItemsPerPage") Optional<Integer> numberOfItemsPerPage) {
		
		return getOrdersByMarketAndUser(marketCode, userId, cursorString, numberOfItemsPerPage);
	}
	
	@Transactional
	@RequestMapping(value = "/orders/**/{marketCode}", method = RequestMethod.POST, consumes = "application/json")
	public void createNewOrder(@PathVariable String marketCode, @RequestBody OrderDto order) {
		// TODO: Identify user
		String user = "Adam";
		orderManagementSystem.createOrder(marketCode, OrderDtoMapping.INSTANCE.fromDto(order), user, createOrderManagementContext());
	}

	@RequestMapping(value = "/myOrders/**/{userId}", method = RequestMethod.GET, produces = { "application/JSON" })
	public PagedOrderListDto getOrdersByUser(@PathVariable String userId, @RequestParam("cursorString") Optional<String> cursorString,
			@RequestParam("numberOfItemsPerPage") Optional<Integer> numberOfItemsPerPage) {
		Integer numberOfItemsPerPageInternal = numberOfItemsPerPage.orElse(10);
		String cursorStringInternal = cursorString.orElse(null);

		PagedOrderList orders = orderDatastore.findOrdersFromCursorBy(userId, cursorStringInternal, numberOfItemsPerPageInternal);
		
		return OrderDtoMapping.INSTANCE.toDto(orders);
	}
	
	@RequestMapping(value = "/myOrders/**/{orderId}/details", method = RequestMethod.GET, produces = { "application/JSON" })
	public OrderDto getOrder(@PathVariable String orderId) {
		return OrderDtoMapping.INSTANCE.toDto(orderManagementSystem.getOrder(Long.valueOf(orderId)).getOrderModel());
	}
	
	@Transactional
	@RequestMapping(value = "/myOrders/**/{orderId}/details", method = RequestMethod.PUT, produces = { "application/JSON" })
	public void updateOrder(@PathVariable String orderId, @RequestBody PropertyChangeWrapperDto propertyChangesDto) {
		// TODO: Identify user
		String user = "Adam";
		orderManagementSystem.changeOrderProperties(Long.valueOf(orderId), propertyChangesDto.getPropertyChanges(), user, createOrderManagementContext());
	}
	
	@Transactional
	@RequestMapping(value = "/myOrders/**/{orderId}/details", method = RequestMethod.DELETE, produces = { "application/JSON" })
	public void deleteOrder(@PathVariable String orderId) {
		// TODO: Identify user
		String user = "Adam";
		orderManagementSystem.deleteOrder(Long.valueOf(orderId), user, createOrderManagementContext());
	}
	
	@RequestMapping(value = "/mySubmittedOrders/**/{userId}", method = RequestMethod.GET, produces = { "application/JSON" })
	public PagedOrderListDto getSubmittedOrdersByUser(@PathVariable String userId, @RequestParam("cursorString") Optional<String> cursorString,
			@RequestParam("numberOfItemsPerPage") Optional<Integer> numberOfItemsPerPage) {
		// TODO: Identify user
		String user = "Adam";
		
		Integer numberOfItemsPerPageInternal = numberOfItemsPerPage.orElse(10);
		String cursorStringInternal = cursorString.orElse(null);
		
		return OrderDtoMapping.INSTANCE.toDto(orderManagementSystem.getActiveOrdersFor(user, cursorStringInternal, numberOfItemsPerPageInternal));
	}
	
	@Transactional
	@RequestMapping(value = "/mySubmittedOrders/**/{userId}", method = RequestMethod.DELETE, produces = { "application/JSON" })
	public void cancelAllOrdersByUser(@PathVariable String userId) {
		// TODO: Identify user
		String user = "Adam";
		
		orderManagementSystem.cancelAllOrdersForUser(user, createOrderManagementContext());
	}
	
	@Transactional
	@RequestMapping(value = "/mySubmittedOrders/**/{userId}", method = RequestMethod.POST, produces = { "application/JSON" })
	public void submitOrder(@PathVariable String userId, @RequestBody SubmitCancelDto submitCancelDto) {
		// TODO: Identify user
		String user = "Adam";
		// TODO: Check the version number 
		orderManagementSystem.submitOrder(Long.valueOf(submitCancelDto.getOrderKey()), user, createOrderManagementContext());
	}
	
	@Transactional
	@RequestMapping(value = "/mySubmittedOrders/**/{orderId}/details", method = RequestMethod.DELETE, produces = { "application/JSON" })
	public void cancelOrder(@PathVariable String orderId) {
		// TODO: Identify user
		String user = "Adam";
		orderManagementSystem.cancelOrder(Long.valueOf(orderId), user, createOrderManagementContext());
	}
	
	public static Pageable createPageable(String cursorString_,
			int numberOnPage_) {
		 return PageRequest.of(cursorString_ != null ? Integer.parseInt(cursorString_) : 0, numberOnPage_);
	}
	
	private PagedOrderListDto getOrdersByMarketAndUser(String marketCode, Optional<String> userId, Optional<String> cursorString,
			Optional<Integer> numberOfItemsPerPage) {
		Integer numberOfItemsPerPageInternal = numberOfItemsPerPage.orElse(10);
		String cursorStringInternal = cursorString.orElse(null);
		
		// check parameters - choice 
		int cnt = 0;
		if(userId.isPresent()) {
			cnt++;
		}

		if(cnt > 1) {
			throw new RuntimeException("Redundant request parameter");
		}
		if(cnt == 0) {
			throw new RuntimeException("Mising request parameter");
		}
		
		PagedOrderList orders = null;
		if(userId.isPresent()) {
			orders = orderDatastore.findOrdersFromCursorBy(userId.get(), marketCode, cursorStringInternal, numberOfItemsPerPageInternal);
		} 
		
		return OrderDtoMapping.INSTANCE.toDto(orders);
	}
	
	// TODO: Create a real implementation here 
	private OrderManagementContext createOrderManagementContext() {
		MarketDataSession marketDataSession = new MockMarketDataSessionImpl(new MarketDataSource() {
			
			@Override
			public void removeMarketDataChangeListener(Long arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void notifyListeners(MarketDataChange[] arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public NotificationBroker getNotificationBroker() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Long addMarketDataChangeListener(String arg0, MarketDataType arg1, PersistedMarketDataChangeListener arg2) {
				// TODO Auto-generated method stub
				return null;
			}
		});
		PropertyChangeSession propertyChangeSession = new MockPropertyChangeSessionImpl(new PropertyChangeBroker() {
			
			@Override
			public void removePropertyChangeListener(Long arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void notifyListeners(PropertyChanges[] arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public NotificationBroker getNotificationBroker() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Long addPropertyChangeListener(DatastoreKey arg0, Class arg1, PersistedPropertyChangeListener arg2) {
				// TODO Auto-generated method stub
				return null;
			}
		});
		TradeNotificationSession tradeNotificationSession = new MockTradeNotificationSessionImpl(new TradeNotificationBroker() {
			
			@Override
			public void notifyListeners(MatchedTradeDto[] arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public NotificationBroker getNotificationBroker() {
				// TODO Auto-generated method stub
				return null;
			}
		});  
		
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
