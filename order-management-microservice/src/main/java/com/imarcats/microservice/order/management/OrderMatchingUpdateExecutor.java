package com.imarcats.microservice.order.management;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.imarcats.interfaces.client.v100.dto.types.MarketDataType;
import com.imarcats.interfaces.client.v100.notification.ListenerCallUserParameters;
import com.imarcats.interfaces.client.v100.notification.MarketDataChange;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;
import com.imarcats.interfaces.server.v100.dto.mapping.MarketDtoMapping;
import com.imarcats.interfaces.server.v100.dto.mapping.PropertyDtoMapping;
import com.imarcats.interfaces.server.v100.order.QuoteOrderPrecedenceRule;
import com.imarcats.internal.server.infrastructure.datastore.MarketDatastore;
import com.imarcats.internal.server.infrastructure.datastore.OrderDatastore;
import com.imarcats.internal.server.interfaces.market.MarketInternal;
import com.imarcats.internal.server.interfaces.order.OrderInternal;
import com.imarcats.internal.server.interfaces.order.OrderManagementContext;
import com.imarcats.microservice.order.management.notification.PropertyChangesMessage;
import com.imarcats.model.BuyBook;
import com.imarcats.model.BuyOrderBookEntry;
import com.imarcats.model.Market;
import com.imarcats.model.Order;
import com.imarcats.model.OrderBookEntryModel;
import com.imarcats.model.OrderBookModel;
import com.imarcats.model.SellBook;
import com.imarcats.model.SellOrderBookEntry;
import com.imarcats.model.mutators.ClientOrderMutator;
import com.imarcats.model.mutators.SystemMarketMutator;
import com.imarcats.model.mutators.SystemOrderMutator;
import com.imarcats.model.types.OrderType;

@Component
public class OrderMatchingUpdateExecutor implements ConsumerSeekAware {
	// We have to set the topic to the one we set up for Kafka Docker - I know,
	// hardcoded topic - again :)
	private static final String IMARCATS_MARKET_CHANGE = "imarcats_market_change";
	private static final String IMARCATS_MARKETDATA = "imarcats_marketdata";
	private static final String IMARCATS_ORDER_CHANGE = "imarcats_order_change";
	
	// action executors 

	// offset management 
	private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();
	private final Map<TopicKey, Integer> offsetMap = java.util.Collections.synchronizedMap(new HashMap<TopicKey, Integer>());
	private CountDownLatch initLatch = new CountDownLatch(1);
	
	@Autowired
	@Qualifier("MarketDatastoreImpl")
	protected MarketDatastore marketDatastore;

	@Autowired
	@Qualifier("OrderDatastoreImpl")
	protected OrderDatastore orderDatastore;

	@Autowired
	protected OrderManagementContext orderManagementContext;


	@PostConstruct
	public void postCreate() {
		initialize();
	}

	private void initialize() {
		// TODO: Initialize in-memory DBs
		
		// TODO: Initialize Offset Map
		
		initLatch.countDown();
	}
	
	@KafkaListener(topicPartitions = @TopicPartition(topic = IMARCATS_MARKET_CHANGE, partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0") }), containerFactory = "kafkaListenerPropertyChangesContainerFactory")
	@Transactional
	public void listenToMarketChangeParition(@Payload PropertyChangesMessage message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header(KafkaHeaders.OFFSET) int offset) {
		processMessage(message.createPropertyChanges(), partition, offset, IMARCATS_MARKET_CHANGE);
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = IMARCATS_ORDER_CHANGE, partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0") }), containerFactory = "kafkaListenerPropertyChangesContainerFactory")
	@Transactional
	public void listenToOrderChangeParition(@Payload PropertyChangesMessage message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header(KafkaHeaders.OFFSET) int offset) {

		processMessage(message.createPropertyChanges(), partition, offset, IMARCATS_ORDER_CHANGE);
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = IMARCATS_MARKETDATA, partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0") }), containerFactory = "kafkaListenerMarketDataChangeContainerFactory")
	@Transactional
	public void listenToMarketDateParition(@Payload MarketDataChange message,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header(KafkaHeaders.OFFSET) int offset) {

		processMessage(message, partition, offset, IMARCATS_MARKETDATA);
	}
	
	
	private void processMessage(ListenerCallUserParameters message, int partition, int offset, String topic) {
		try {
			// wait for the init 
			initLatch.await();
			
			// check offset for partition 
			Integer offsetFromMap = offsetMap.get(new TopicKey(topic, partition));
			if(offsetFromMap != null) {
				int intendedOffset = offsetFromMap + 1;
				// check, if we should have already consumed the message 
				if (intendedOffset > offset) {
					seekCallBack.get().seek(topic, partition, intendedOffset);
					return;
				}
			}
			
			processMessage(message);
			
			// save the offset for the last successfully processed message 
			offsetMap.put(new TopicKey(topic, partition), offset);
			
		} catch (InterruptedException e) {
			// TODO Log it correctly 
			e.printStackTrace();
		}
	}

	private void processMessage(ListenerCallUserParameters message) {
		try {
			if (message instanceof MarketDataChange) {
				processMessage((MarketDataChange) message);
			} else if (message instanceof PropertyChanges) {
				processMessage((PropertyChanges) message); 
			}
		} catch (Exception e) {
			// critical system error - stop system 

			// TODO: Add proper logging
			System.out.println("Critical error during processing message: " + e + " - stopping Order Management system");
			System.exit(1); 
		}
	}


	private void processMessage(MarketDataChange message) {
		MarketInternal marketInternal = marketDatastore.findMarketBy(message.getMarketCode());
		if (marketInternal != null) {						
			MarketDataType changeType = message.getChangeType();
			switch (changeType) {
			case Open:
				marketInternal.getMarketModel().setOpeningQuote(MarketDtoMapping.INSTANCE.fromDto(message.getNewQuote()));
				break;
			case Close:
				marketInternal.getMarketModel().setClosingQuote(MarketDtoMapping.INSTANCE.fromDto(message.getNewQuote()));
				break;
			case Bid:
				marketInternal.getMarketModel().setCurrentBestBid(MarketDtoMapping.INSTANCE.fromDto(message.getNewQuoteAndSize()));				
				break;
			case Ask:
				marketInternal.getMarketModel().setCurrentBestAsk(MarketDtoMapping.INSTANCE.fromDto(message.getNewQuoteAndSize()));
				break;
			case Last:
				marketInternal.getMarketModel().setLastTrade(MarketDtoMapping.INSTANCE.fromDto(message.getNewQuoteAndSize()));
				break;
			case LevelIIAsk:
				SellBook sellBook = marketInternal.getMarketModel().getSellBook();
				if (sellBook == null) {
					sellBook = new SellBook();
					marketInternal.getMarketModel().setSellBook(sellBook);
				}
				OrderBookEntryModel sellEntryModel = new SellOrderBookEntry();
				updateLevel2MarketData(message, marketInternal, sellBook, sellEntryModel);
				break;
			case LevelIIBid:
				BuyBook buyBook = marketInternal.getMarketModel().getBuyBook();
				if (buyBook == null) {
					buyBook = new BuyBook();
					marketInternal.getMarketModel().setBuyBook(buyBook);
				}
				OrderBookEntryModel buyEntryModel = new BuyOrderBookEntry();
				updateLevel2MarketData(message, marketInternal, buyBook, buyEntryModel);
				break;
				
			default:
				break;
			}
		}			
		
	}

	private void updateLevel2MarketData(MarketDataChange message, MarketInternal marketInternal, OrderBookModel book,
			OrderBookEntryModel entryModel) {
		int newQuoteSize = message.getNewQuoteSize();
		entryModel.setAggregateSize(newQuoteSize);
		entryModel.setLimitQuote(MarketDtoMapping.INSTANCE.fromDto(message.getNewQuote()));
		entryModel.setOrderType(OrderType.Limit);
		entryModel.updateLastUpdateTimestamp();
		int index = book.binarySearch(entryModel, new QuoteOrderPrecedenceRule(book.getSide(), 
				marketInternal.getMinimumQuoteIncrement(), marketInternal.getQuoteType()));
		if (index >= 0) {
			book.remove(entryModel);
		}
		if (newQuoteSize > 0) {
			book.add(entryModel);
		}
	}
	
	private void processMessage(PropertyChanges message) {
		if (message.getClassBeingChanged().equals(Market.class)) {
			MarketInternal marketInternal = marketDatastore.findMarketBy(message.getObjectBeingChanged().getCodeKey());
			if (marketInternal != null) {				
				SystemMarketMutator.INSTANCE.executePropertyChanges(marketInternal.getMarketModel(), PropertyDtoMapping.INSTANCE.fromDto(message.getChanges()), null);
			}
		} else if (message.getClassBeingChanged().equals(Order.class)) {
			OrderInternal orderInternal = orderDatastore.findOrderBy(message.getObjectBeingChanged().getIdKey());
			if (orderInternal != null) {				
				SystemOrderMutator.INSTANCE.executePropertyChanges(orderInternal.getOrderModel(), PropertyDtoMapping.INSTANCE.fromDto(message.getChanges()), null);
			}
		}
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		seekCallBack.set(callback);
	}

	@Override
	public void onPartitionsAssigned(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
			ConsumerSeekCallback callback) {
		// do nothing 
	}

	@Override
	public void onIdleContainer(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
			ConsumerSeekCallback callback) {
		// do nothing
	}
	
	// classes 
	private class TopicKey {
		private final String topic;
		private final Integer partition;

		public TopicKey(String topic, Integer partition) {
			super();
			this.topic = topic;
			this.partition = partition;
		}

		public String getTopic() {
			return topic;
		}

		public Integer getPartition() {
			return partition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((partition == null) ? 0 : partition.hashCode());
			result = prime * result + ((topic == null) ? 0 : topic.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TopicKey other = (TopicKey) obj;
			if (partition == null) {
				if (other.partition != null)
					return false;
			} else if (!partition.equals(other.partition))
				return false;
			if (topic == null) {
				if (other.topic != null)
					return false;
			} else if (!topic.equals(other.topic))
				return false;
			return true;
		}

		
		
	}
}
