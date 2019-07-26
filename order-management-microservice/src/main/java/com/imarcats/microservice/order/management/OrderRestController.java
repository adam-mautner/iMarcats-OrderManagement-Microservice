package com.imarcats.microservice.order.management;

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

import com.imarcats.interfaces.client.v100.dto.OrderDto;
import com.imarcats.interfaces.client.v100.dto.types.PagedOrderListDto;
import com.imarcats.interfaces.server.v100.dto.mapping.MarketDtoMapping;
import com.imarcats.interfaces.server.v100.dto.mapping.OrderDtoMapping;
import com.imarcats.internal.server.infrastructure.datastore.MarketDatastore;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.interfaces.market.MarketInternal;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.market.engine.order.OrderManagementSystem;
import com.imarcats.model.types.PagedOrderList;

@RestController
@RequestMapping("/")
public class OrderRestController {

	@Autowired
	protected OrderManagementSystem orderManagementSystem;
	
	@Autowired
	@Qualifier("OrderDatastoreImpl")
	protected OrderDatastore orderDatastore;

	@Autowired
	protected OrderManagementContext orderManagementContext;
	
	@Autowired
	protected ActionRequestor actionRequestor;
	
	@Autowired
	@Qualifier("MarketDatastoreImpl")
	protected MarketDatastore marketDatastore;
	
	@RequestMapping(value = "/orders/**/{marketCode}", method = RequestMethod.GET, produces = { "application/JSON" })
	public PagedOrderListDto getOrdersByMarket(@PathVariable String marketCode, 
			@RequestParam("userId") Optional<String> userId,
			@RequestParam("cursorString") Optional<String> cursorString,
			@RequestParam("numberOfItemsPerPage") Optional<Integer> numberOfItemsPerPage) {
		
		return getOrdersByMarketAndUser(marketCode, userId, cursorString, numberOfItemsPerPage);
	}
	
	@Transactional
	@RequestMapping(value = "/orders/**/{marketCode}", method = RequestMethod.POST, consumes = "application/json")
	public long createNewOrder(@PathVariable String marketCode, @RequestBody OrderDto order) {
		// TODO: Identify user
		String user = "Adam";
		return orderManagementSystem.createOrder(marketCode, OrderDtoMapping.INSTANCE.fromDto(order), user, orderManagementContext);
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
		orderManagementSystem.changeOrderProperties(Long.valueOf(orderId), propertyChangesDto.getPropertyChanges(), user, orderManagementContext);
	}
	
	@Transactional
	@RequestMapping(value = "/myOrders/**/{orderId}/details", method = RequestMethod.DELETE, produces = { "application/JSON" })
	public void deleteOrder(@PathVariable String orderId) {
		// TODO: Identify user
		String user = "Adam";
		orderManagementSystem.deleteOrder(Long.valueOf(orderId), user, orderManagementContext);
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
		
		orderManagementSystem.cancelAllOrdersForUser(user, orderManagementContext);
	}
	
	@Transactional
	@RequestMapping(value = "/mySubmittedOrders/**/{userId}", method = RequestMethod.POST, produces = { "application/JSON" })
	public void submitOrder(@PathVariable String userId, @RequestBody SubmitCancelDto submitCancelDto) {
		// TODO: Identify user
		String user = "Adam";
		// TODO: Check the version number 
		orderManagementSystem.submitOrder(Long.valueOf(submitCancelDto.getOrderKey()), user, orderManagementContext);
	}
	
	@Transactional
	@RequestMapping(value = "/mySubmittedOrders/**/{orderId}/details", method = RequestMethod.DELETE, produces = { "application/JSON" })
	public void cancelOrder(@PathVariable String orderId) {
		// TODO: Identify user
		String user = "Adam";
		orderManagementSystem.cancelOrder(Long.valueOf(orderId), user, orderManagementContext);
	}
	
	@Transactional
	@RequestMapping(value = "/activeMarkets", method = RequestMethod.POST, consumes = "application/json")
	public void activateMarket(@RequestBody MarketActionDto activationDto) {
		// TODO: Later we will receive the market here and save it to DB
		MarketInternal marketInternal = marketDatastore.findMarketBy(activationDto.getCode());
		actionRequestor.createActiveMarket(MarketDtoMapping.INSTANCE.toDto(marketInternal.getMarketModel()));
	}
	
	@Transactional
	@RequestMapping(value = "/activeMarkets/{MarketCode}", method = RequestMethod.DELETE)
	public void deactivateMarket(@PathVariable String marketCode) {
		// TODO: Later we will delete the market here 
		actionRequestor.deleteActiveMarket(marketCode);
	}

	@Transactional
	@RequestMapping(value = "/openMarkets", method = RequestMethod.POST, consumes = "application/json")
	public void openMarket(@RequestBody MarketActionDto openDto) {
		// TODO: Later we will hate to mark the market open in the DB
		actionRequestor.openMarket(openDto.getCode());
	}
	
	@Transactional
	@RequestMapping(value = "/openMarkets/{MarketCode}", method = RequestMethod.DELETE)
	public void closeMarket(@PathVariable String marketCode) {
		// TODO: Later we will hate to mark the market closed in the DB
		actionRequestor.closeMarket(marketCode);
	}

	@Transactional
	@RequestMapping(value = "/calledMarkets", method = RequestMethod.POST, consumes = "application/json")
	public void callMarket(@RequestBody MarketActionDto callDto) {
		actionRequestor.callMarket(callDto.getCode(), callDto.getNextCallDate(), callDto.getNextMarketCallTime());
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
}
