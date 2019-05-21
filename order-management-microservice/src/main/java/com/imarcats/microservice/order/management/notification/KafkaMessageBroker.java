package com.imarcats.microservice.order.management.notification;

import org.springframework.stereotype.Component;

import com.imarcats.interfaces.client.v100.notification.ListenerCallUserParameters;
import com.imarcats.interfaces.client.v100.notification.NotificationType;
import com.imarcats.model.types.DatastoreKey;

@Component
public class KafkaMessageBroker {

	@SuppressWarnings("unchecked")
	public void notifyListeners(DatastoreKey observedObject_, 
			Class observedObjectClass_, 
			NotificationType notificationType_, 
			String filterString_, 
			ListenerCallUserParameters parameters_) {
		// Does nothing as no update messages produced from order management 
	}
 
}

