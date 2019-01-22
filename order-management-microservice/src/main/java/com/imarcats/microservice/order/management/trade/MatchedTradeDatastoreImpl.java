package com.imarcats.microservice.order.management.trade;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;

import com.imarcats.internal.server.infrastructure.datastore.MatchedTradeDatastore;
import com.imarcats.microservice.order.management.OrderRestController;
import com.imarcats.model.MatchedTrade;
import com.imarcats.model.TradeSide;
import com.imarcats.model.types.PagedMatchedTradeSideList;

@Component("MatchedTradeDatastoreImpl")
public class MatchedTradeDatastoreImpl implements MatchedTradeDatastore {

	@Autowired
	private MatchedTradeCrudRepository tradeCrudRepository;
	
	@Autowired
	private MatchedTradeJpaRepository tradeJpaRepository;
	
	@Override
	public Long createMatchedTrade(MatchedTrade matchedTrade_) {
		return tradeCrudRepository.save(matchedTrade_).getTransactionID();
	}

	@Override
	public MatchedTrade[] findAllMatchedTradeByMarket(String marketCode_) {
		return toMatchedTradeArray(tradeJpaRepository.findAllMatchedTradeByMarket(marketCode_));
	}

	@Override
	public TradeSide findMatchedTradeByReferenceUserAndMarket(String externalReference_, String userID_, String marketCode_) {
		return tradeJpaRepository.findMatchedTradeByReferenceUserAndMarket(externalReference_, userID_, marketCode_).orElse(null);
	}

	@Override
	public MatchedTrade findMatchedTradeByTransactionId(Long transactionId_) {
		return tradeCrudRepository.findById(transactionId_).orElse(null); 
	}

	@Override
	public PagedMatchedTradeSideList findMatchedTradeByUser(String userID_, String cursorString_, int maxNumberOfMatchedTradeSidesOnPage_) {
		return createPagedTradeSideList(tradeJpaRepository.findMatchedTradeByUser(userID_, OrderRestController.createPageable(cursorString_, maxNumberOfMatchedTradeSidesOnPage_)));
	}

	@Override
	public PagedMatchedTradeSideList findMatchedTradeByUserAndMarket(String userID_, String marketCode_, String cursorString_, int maxNumberOfMatchedTradeSidesOnPage_) {
		return createPagedTradeSideList(tradeJpaRepository.findMatchedTradeByUserAndMarket(userID_, marketCode_, OrderRestController.createPageable(cursorString_, maxNumberOfMatchedTradeSidesOnPage_)));	
	}

	@Override
	public TradeSide[] findMatchedTradeByUserAndMarketInternal(String userID_, String marketCode_) {
		return toTradeSideArray(tradeJpaRepository.findMatchedTradeByUserAndMarketInternal(userID_, marketCode_));
	}

	@Override
	public TradeSide[] findMatchedTradeByUserInternal(String userID_) {
		return toTradeSideArray(tradeJpaRepository.findMatchedTradeByUserInternal(userID_));	
	}
	
	private MatchedTrade[] toMatchedTradeArray(List<MatchedTrade> trades) {
		return trades.toArray(new MatchedTrade[trades.size()]);
	}

	private TradeSide[] toTradeSideArray(List<TradeSide> trades) {
		return trades.toArray(new TradeSide[trades.size()]);
	}

	
	private PagedMatchedTradeSideList createPagedTradeSideList(Page<TradeSide> page) {
		PagedMatchedTradeSideList list = new PagedMatchedTradeSideList();
		list.setMatchedTradeSides(page.getContent().toArray(new TradeSide[page.getContent().size()]));
		list.setCursorString(""+(page.getNumber() + 1));
		list.setMaxNumberOfMatchedTradeSidesOnPage(page.getNumberOfElements());
		 
		return list;
	}
}
