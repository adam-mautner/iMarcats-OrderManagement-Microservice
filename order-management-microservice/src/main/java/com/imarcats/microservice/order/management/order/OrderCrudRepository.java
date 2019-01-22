package com.imarcats.microservice.order.management.order;

import org.springframework.data.repository.CrudRepository;

import com.imarcats.model.Order;

public interface OrderCrudRepository extends CrudRepository<Order, Long> {

}

