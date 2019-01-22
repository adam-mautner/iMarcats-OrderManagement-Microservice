package com.imarcats.microservice.order.management.trade;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.imarcats.model.Market;
import com.imarcats.model.MatchedTrade;
import com.imarcats.model.TradeSide;

public interface MatchedTradeJpaRepository extends JpaRepository<Market, String> {

	public static final String SELECT_TRADE_SIDE_BY_USER_AND_MKT = "SELECT t FROM TradeSide t where t._traderID=?1 and t._marketOfTheTrade=?2 ORDER BY t._tradeDateTime desc";
	public static final String SELECT_TRADE_SIDE_BY_USER = "SELECT t FROM TradeSide t where t._traderID=?1 ORDER BY t._tradeDateTime desc";

	@Query(value = "SELECT t FROM MatchedTrade t where t._marketOfTheTrade=?1 ORDER BY t._transactionID desc")
	public List<MatchedTrade> findAllMatchedTradeByMarket(String marketCode_);

	@Query(value = "SELECT t FROM TradeSide t where t._externalOrderReference=?1 and t._traderID=?2 and t._marketOfTheTrade=?3 ORDER BY t._tradeDateTime desc")
	public Optional<TradeSide> findMatchedTradeByReferenceUserAndMarket(String externalReference_, String userID_,
			String marketCode_);

	@Query(value = SELECT_TRADE_SIDE_BY_USER)
	public Page<TradeSide> findMatchedTradeByUser(String userID_, Pageable createPageable);

	@Query(value = SELECT_TRADE_SIDE_BY_USER_AND_MKT)
	public Page<TradeSide> findMatchedTradeByUserAndMarket(String userID_, String marketCode_, Pageable createPageable);

	@Query(value = SELECT_TRADE_SIDE_BY_USER_AND_MKT)
	public List<TradeSide> findMatchedTradeByUserAndMarketInternal(String userID_, String marketCode_);

	@Query(value = SELECT_TRADE_SIDE_BY_USER)
	public List<TradeSide> findMatchedTradeByUserInternal(String userID_);

}
