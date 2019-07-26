package com.imarcats.microservice.order.management;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.imarcats.interfaces.client.v100.notification.MarketDataChange;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;
import com.imarcats.microservice.order.management.notification.JsonPOJODeserializer;
import com.imarcats.microservice.order.management.notification.JsonPOJOSerializer;
import com.imarcats.microservice.order.management.notification.PropertyChangesMessage;

@EnableKafka
@Configuration
public class KafkaConfig {

	// I know, I know, but for now we will use hardcoded address here
	// private String bootstrapAddress = "192.168.99.100:9092"; // VirtualBox Docker
	private String bootstrapAddress = "10.0.75.1:9092"; // Windows Docker

    // producers         
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
	
	// consumers
	@Bean
	@ConditionalOnMissingBean(name = "kafkaListenerPropertyChangesContainerFactory")
	public ConsumerFactory<String, PropertyChangesMessage> propertyChangesConsumerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		setDefaults(configProps);
		configProps.put("JsonPOJOClass", PropertyChangesMessage.class);
		return new DefaultKafkaConsumerFactory<>(configProps);
	}

	@Bean(name="kafkaListenerPropertyChangesContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, PropertyChangesMessage> kafkaListenerPropertyChangesContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, PropertyChangesMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(propertyChangesConsumerFactory());
		return factory;
	}
	
	@Bean
	@ConditionalOnMissingBean(name = "kafkaListenerMarketDataChangeContainerFactory")
	public ConsumerFactory<String, MarketDataChange> marketDataChangeConsumerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		setDefaults(configProps);
		configProps.put("JsonPOJOClass", MarketDataChange.class);
		return new DefaultKafkaConsumerFactory<>(configProps);
	}

	@Bean(name="kafkaListenerMarketDataChangeContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, MarketDataChange> kafkaListenerMarketDataChangeContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, MarketDataChange> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(marketDataChangeConsumerFactory());
		return factory;
	}

	// defaults
	private void setDefaults(Map<String, Object> configProps) {
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class);
		configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonPOJODeserializer.class);
	}
}
