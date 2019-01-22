package com.imarcats.microservice.order.management.order;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;

import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.interfaces.order.OrderInternal;
import com.imarcats.market.engine.order.OrderImpl;
import com.imarcats.microservice.order.management.OrderRestController;
import com.imarcats.microservice.order.management.market.MarketDatastoreImpl;
import com.imarcats.model.Order;
import com.imarcats.model.types.PagedOrderList;

@Component("OrderDatastoreImpl")
public class OrderDatastoreImpl implements OrderDatastore {

	@Autowired
	private OrderCrudRepository orderCrudRepository;
	
	@Autowired
	private OrderJpaRepository orderJpaRepository;
	
	@Autowired
	private MarketDatastoreImpl marketDatastore;

	@PostConstruct
	public void postCreate() {
		marketDatastore.setOrderDatastore(this);
	}
	
	@Override
	public Long createOrder(Order order) {
		return orderCrudRepository.save(order).getKey();
	}

	@Override
	public void deleteOrder(Long orderKey) {
		orderCrudRepository.deleteById(orderKey);
	}
	
	@Override
	public OrderInternal findOrderBy(Long orderKey) {
		Optional<Order> order = orderCrudRepository.findById(orderKey);
		OrderInternal orderInternal = toOrderInternal(order);
		return orderInternal;
	}

	@Override
	public void deleteNonActiveOrdersOnMarket(String marketCode) {
		Arrays.stream(findNonActiveOrdersOnMarket(marketCode)).forEach(o -> deleteOrder(o.getKey()));
	}

	@Override
	public PagedOrderList findActiveOrdersFromCursorBy(String userID_, String cursorString_, int maxNumberOfOrderOnPage_) {
		return createPagedOrderList(orderJpaRepository.findActiveOrdersFromCursorBy(userID_, OrderRestController.createPageable(cursorString_, maxNumberOfOrderOnPage_)));
	}

	@Override
	public OrderInternal[] findActiveOrdersOnMarket(String marketCode_) {
		List<Order> orders = orderJpaRepository.findActiveOrdersOnMarket(marketCode_);
		return toOrderInternalArray(orders);
	}

	@Override
	public OrderInternal[] findNonActiveOrdersOnMarket(String marketCode_) {
		List<Order> orders = orderJpaRepository.findNonActiveOrdersOnMarket(marketCode_);
		return toOrderInternalArray(orders);
	}

	@Override
	public OrderInternal findOrderBy(String externalReference_, String userID_, String marketCode_) {
		return toOrderInternal(orderJpaRepository.findOrderBy(externalReference_, userID_, marketCode_));
	}

	@Override
	public OrderInternal[] findOrdersBy(String userID_, String marketCode_) {
		List<Order> orders = orderJpaRepository.findOrdersBy(userID_, marketCode_);
		return toOrderInternalArray(orders);
	}

	@Override
	public OrderInternal[] findOrdersBy(String userID_) {
		List<Order> orders = orderJpaRepository.findOrdersBy(userID_);
		return toOrderInternalArray(orders);
	}

	@Override
	public PagedOrderList findOrdersFromCursorBy(String userID_, String cursorString_, int maxNumberOfOrderOnPage_) {
		return createPagedOrderList(orderJpaRepository.findOrdersFromCursorBy(userID_, OrderRestController.createPageable(cursorString_, maxNumberOfOrderOnPage_)));
	}

	@Override
	public PagedOrderList findOrdersFromCursorBy(String userID_, String marketCode_, String cursorString_, int maxNumberOfOrderOnPage_) {
		return createPagedOrderList(orderJpaRepository.findOrdersFromCursorBy(userID_, marketCode_, OrderRestController.createPageable(cursorString_, maxNumberOfOrderOnPage_)));
	}

	private OrderInternal[] toOrderInternalArray(List<Order> orders) {
		List<OrderInternal> orderInternals = orders.stream().map(o -> toOrderInternal(o)).collect(Collectors.toList());
		return orderInternals.toArray(new OrderInternal[orderInternals.size()]);
	}
	
	private OrderInternal toOrderInternal(Optional<Order> order) {
		OrderInternal orderInternal = null;
		if (order.isPresent()) {
			Order orderModel = order.get();
			orderInternal = toOrderInternal(orderModel);
		}
		return orderInternal;
	}

	private OrderImpl toOrderInternal(Order orderModel) {
		return new OrderImpl(orderModel, marketDatastore);
	}

	private PagedOrderList createPagedOrderList(Page<Order> page) {
		PagedOrderList list = new PagedOrderList();
		list.setOrders(page.getContent().toArray(new Order[page.getContent().size()]));
		list.setCursorString(""+(page.getNumber() + 1));
		list.setMaxNumberOfOrdersOnPage(page.getNumberOfElements());
		 
		return list;
	}
}
