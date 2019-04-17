package com.imarcats.microservice.order.management;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.imarcats.interfaces.client.v100.dto.types.TradeSideDto;
import com.imarcats.interfaces.client.v100.notification.MarketDataChange;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;
import com.imarcats.microservice.order.management.notification.JsonPOJODeserializer;
import com.imarcats.microservice.order.management.notification.JsonPOJOSerializer;
import com.imarcats.microservice.order.management.notification.TradeActionMessage;

@Configuration
public class KafkaConfig {

	// I know, I know, but for now we will use hardcoded address here 
    private String bootstrapAddress = "192.168.99.100:9092"; 

    // producers 
	@Bean
    public ProducerFactory<String, MarketDataChange> producerMarketDataChangeFactory() {
        Map<String, Object> configProps = new HashMap<>();
        setDefaults(configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
	
    @Bean
    public KafkaTemplate<String, MarketDataChange> kafkaMarketDataChangeTemplate() {
        return new KafkaTemplate<>(producerMarketDataChangeFactory());
    }

	@Bean
    public ProducerFactory<String, PropertyChanges> producerPropertyChangesFactory() {
        Map<String, Object> configProps = new HashMap<>();
        setDefaults(configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, TradeSideDto> kafkaTradeSideDtoTemplate() {
        return new KafkaTemplate<>(producerTradeSideDtoFactory());
    }
    
	@Bean
    public ProducerFactory<String, TradeSideDto> producerTradeSideDtoFactory() {
        Map<String, Object> configProps = new HashMap<>();
        setDefaults(configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
	
    @Bean
    public KafkaTemplate<String, PropertyChanges> kafkaPropertyChangesTemplate() {
        return new KafkaTemplate<>(producerPropertyChangesFactory());
    }
    
	@Bean
    public ProducerFactory<String, TradeActionMessage> producerTradeActionMessageFactory() {
        Map<String, Object> configProps = new HashMap<>();
        setDefaults(configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, TradeActionMessage> kafkaTradeActionMessageTemplate() {
        return new KafkaTemplate<>(producerTradeActionMessageFactory());
    }
    
    @Bean
    public KafkaTemplate<String, UpdateMessage> kafkaUpdateMessageTemplate() {
        return new KafkaTemplate<>(producerUpdateMessageFactory());
    }
    
	@Bean
    public ProducerFactory<String, UpdateMessage> producerUpdateMessageFactory() {
        Map<String, Object> configProps = new HashMap<>();
        setDefaults(configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
    
	// consumer 
    @Bean
    public ConsumerFactory<String, UpdateMessage> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        setDefaults(configProps);
        configProps.put(
        	"JsonPOJOClass", 
        	UpdateMessage.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UpdateMessage> 
      kafkaListenerContainerFactory() {
    
        ConcurrentKafkaListenerContainerFactory<String, UpdateMessage> factory
          = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
	
    // defaults 
	private void setDefaults(Map<String, Object> configProps) {
		configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          bootstrapAddress);
        configProps.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
          StringSerializer.class);
        configProps.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
          JsonPOJOSerializer.class);
        configProps.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        configProps.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          JsonPOJODeserializer.class);
	}
    
}
