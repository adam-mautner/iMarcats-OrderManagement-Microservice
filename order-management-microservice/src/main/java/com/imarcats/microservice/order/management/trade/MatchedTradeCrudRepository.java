package com.imarcats.microservice.order.management.trade;

import org.springframework.data.repository.CrudRepository;

import com.imarcats.model.MatchedTrade;

public interface MatchedTradeCrudRepository extends CrudRepository<MatchedTrade, Long> {

}
