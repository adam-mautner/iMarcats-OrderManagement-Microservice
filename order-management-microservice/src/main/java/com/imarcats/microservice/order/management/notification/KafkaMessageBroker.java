package com.imarcats.microservice.order.management.notification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.imarcats.interfaces.client.v100.dto.types.TradeSideDto;
import com.imarcats.interfaces.client.v100.notification.ListenerCallUserParameters;
import com.imarcats.interfaces.client.v100.notification.MarketDataChange;
import com.imarcats.interfaces.client.v100.notification.NotificationType;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;
import com.imarcats.internal.server.infrastructure.notification.trades.TradeNotification;
import com.imarcats.model.Market;
import com.imarcats.model.Order;
import com.imarcats.model.types.DatastoreKey;

@Component
public class KafkaMessageBroker {
	
	@Autowired
	private KafkaTemplate<String, MarketDataChange> marketDataChangeKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, PropertyChanges> propertyChangesKafkaTemplate;
	
	@Autowired
	private KafkaTemplate<String, TradeActionMessage> tradeActionMessageKafkaTemplate;

	@Autowired
	private KafkaTemplate<String, TradeSideDto> tradeSideKafkaTemplate;
	
	// We have to set the topic to the one we set up for Kafka Docker - I know,
	// hardcoded topic - again :)
	private static final String IMARCATS_MARKET_CHANGE = "imarcats_market_change";
	private static final String IMARCATS_MARKETDATA = "imarcats_marketdata";
	private static final String IMARCATS_ORDER_CHANGE = "imarcats_order_change";
	private static final String IMARCATS_TRADES = "imarcats_trades";

	private static final String IMARCATS_TRADES_TRASACTIONS = "imarcats_trade_tx";
	
	@SuppressWarnings("unchecked")
	public void notifyListeners(DatastoreKey observedObject_, 
			Class observedObjectClass_, 
			NotificationType notificationType_, 
			String filterString_, 
			ListenerCallUserParameters parameters_) {
		// TODO: Simplify
//		ListenerCall call = 
//			new ListenerCall(UniqueIDGenerator.nextID(),
//					parameters_, observedObject_, 
//					observedObjectClass_.getName(), notificationType_, 
//					filterString_, new Date());
				
		// send notification to client
		// TODO: rework this 
		if (parameters_ instanceof TradeNotification) {
			TradeNotification tradeNotification = (TradeNotification) parameters_;
			
			 sendTradeNotification(tradeNotification.getTrade().getBuySide());
			 sendTradeNotification(tradeNotification.getTrade().getSellSide());
			 
//			_tradeHandler.notifyListeners(new ListenerCallParameters(call.getId(), call, filterString_, call.getListenerCallTimestamp(), "callTask"));			
		} else if(parameters_ instanceof PropertyChanges) {
			PropertyChanges propertyChanges = (PropertyChanges) parameters_;
			
			sendPropertyChangeNotification(observedObject_, observedObjectClass_,
					propertyChanges);
			
//			_changeHandler.notifyListeners(new ListenerCallParameters(call.getId(), call, filterString_, call.getListenerCallTimestamp(), "callTask"));
		} else if(parameters_ instanceof MarketDataChange) {
			MarketDataChange marketDataChange = (MarketDataChange) parameters_;
			marketDataChangeKafkaTemplate.send(IMARCATS_MARKETDATA, marketDataChange);
			
//			_changeHandler.notifyListeners(new ListenerCallParameters(call.getId(), call, filterString_, call.getListenerCallTimestamp(), "callTask"));
		}
	}

//	private void sendMarketDataNotification(MarketDataChange marketDataChange) {
//		sendMessageToTopicForMarket(marketDataChange, _marketDataTopic, marketDataChange.getMarketCode());
//		
//		// TODO: Add notification queues 
//	}

	private void sendPropertyChangeNotification(DatastoreKey observedObject_, Class observedObjectClass_,
			PropertyChanges propertyChanges) {
		if(Market.class.equals(observedObjectClass_)) {
			propertyChangesKafkaTemplate.send(IMARCATS_MARKET_CHANGE, createMarketKey(propertyChanges.getParentObject().getCodeKey()), propertyChanges);
		} else if(Order.class.equals(observedObjectClass_)) {
			propertyChangesKafkaTemplate.send(IMARCATS_ORDER_CHANGE, createMarketUserKey(propertyChanges.getParentObject().getCodeKey(), propertyChanges.getObjectOwner()), propertyChanges);
			propertyChangesKafkaTemplate.send(IMARCATS_ORDER_CHANGE, createMarketKey(propertyChanges.getParentObject().getCodeKey()), propertyChanges);
			
		}
		
		// TODO: Add notification queues 
	}

	private void sendTradeNotification(TradeSideDto tradeSide_) {
		tradeSideKafkaTemplate.send(IMARCATS_TRADES, createMarketUserKey(tradeSide_.getMarketOfTheTrade(), tradeSide_.getTraderID()), tradeSide_);
		tradeSideKafkaTemplate.send(IMARCATS_TRADES, createUserKey(tradeSide_.getTraderID()), tradeSide_);
		
		TradeActionMessage tradeActionMessage = new TradeActionMessage();
		tradeActionMessage.setTransactionID(tradeSide_.getTransactionID());
		tradeActionMessage.setMarketCode(tradeSide_.getMarketOfTheTrade());
		tradeActionMessage.setSide(tradeSide_.getSide()); 
		
		tradeActionMessageKafkaTemplate.send(IMARCATS_TRADES_TRASACTIONS, tradeActionMessage);
	}

	private String createMarketUserKey(String market, String user) {
		return market + "-" + user;
	}
	
	private String createUserKey(String user) {
		return user;
	}
	
	private String createMarketKey(String market) {
		return market;
	}

	
//	@SuppressWarnings("unchecked")
//	public Long addListener(DatastoreKey observedObject_,
//			Class observedObjectClass_,
//			NotificationType notificationType_, 
//			String filterString_,
//			PersistedListener listener_) {
//		setupListener(observedObject_, observedObjectClass_, notificationType_, 
//				filterString_, listener_);
//		
//		// save to datastore
//		return _listenerDatastore.createListener(listener_);
//	}
//	
//	@SuppressWarnings("unchecked")
//	private void setupListener(DatastoreKey observedObject_,
//			Class observedObjectClass_, 
//			NotificationType notificationType_,
//			String filterString_,
//			PersistedListener listener_) {
//		// set observed object and filter string
//		listener_.setObservedObjectKey(observedObject_);
//		listener_.setObservedClassName(observedObjectClass_.getName());
//		listener_.setNotificationType(notificationType_);
//		listener_.setFilterString(filterString_);
//	}
//
//	public void removeListener(Long listenerKey_) {
//		_listenerDatastore.deleteListener(listenerKey_);
//	}	
//
//	@SuppressWarnings("unchecked")
//	public void removeAllListeners(DatastoreKey observedObjectKey_, Class observedObjectClass_) {
//		_listenerDatastore.deleteAllListeners(observedObjectKey_, observedObjectClass_.getName());
//	} 
}

