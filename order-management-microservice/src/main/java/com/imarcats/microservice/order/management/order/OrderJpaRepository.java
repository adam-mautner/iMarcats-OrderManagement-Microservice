package com.imarcats.microservice.order.management.order;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.imarcats.model.Order;

public interface OrderJpaRepository extends JpaRepository<Order, Long> {

	public static final String SELECT_BY_USER_AND_MKT = "SELECT o FROM Order o where o._submitterID=?1 and o._targetMarketCode=?2 ORDER BY o._submissionDate desc";
	public static final String SELECT_BY_SUBMITTER = "SELECT o FROM Order o where o._submitterID=?1 ORDER BY o._submissionDate desc";

	@Query(value = SELECT_BY_SUBMITTER)
	public Page<Order> findActiveOrdersFromCursorBy(String userID_, Pageable pageable);

	@Query(value = "SELECT o FROM Order o where o._targetMarketCode=?1 and o._state in ('PendingSubmit', 'WaitingSubmit', 'Submitted') ORDER BY o._submissionDate desc")
	public List<Order> findActiveOrdersOnMarket(String marketCode_);
	
	@Query(value = "SELECT o FROM Order o where o._targetMarketCode=?1 and o._state not in ('PendingSubmit', 'WaitingSubmit', 'Submitted') ORDER BY o._submissionDate desc")
	public List<Order> findNonActiveOrdersOnMarket(String marketCode_);	
	
	@Query(value = "SELECT o FROM Order o where o._externalOrderReference=?1 and o._submitterID=?2 and o._targetMarketCode=?3 ORDER BY o._submissionDate desc")
	public Optional<Order> findOrderBy(String externalReference_, String userID_, String marketCode_);
	
	@Query(value = SELECT_BY_USER_AND_MKT)
	public List<Order> findOrdersBy(String userID_, String marketCode_);
	
	@Query(value = SELECT_BY_SUBMITTER)
	public List<Order> findOrdersBy(String userID_);
	
	@Query(value = SELECT_BY_SUBMITTER)
	public Page<Order> findOrdersFromCursorBy(String userID_, Pageable pageable);
	
	@Query(value = SELECT_BY_USER_AND_MKT)
	public Page<Order> findOrdersFromCursorBy(String userID_, String marketCode_, Pageable pageable);
}
