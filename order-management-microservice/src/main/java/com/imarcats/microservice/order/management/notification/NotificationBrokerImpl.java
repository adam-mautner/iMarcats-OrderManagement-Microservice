package com.imarcats.microservice.order.management.notification;

import com.imarcats.interfaces.client.v100.notification.ListenerCallUserParameters;
import com.imarcats.interfaces.client.v100.notification.NotificationType;
import com.imarcats.internal.server.infrastructure.notification.NotificationBroker;
import com.imarcats.internal.server.infrastructure.notification.PersistedListener;
import com.imarcats.model.types.DatastoreKey;

public class NotificationBrokerImpl implements NotificationBroker {

	private JmsMessageBroker kafkaMessageBroker;

	public NotificationBrokerImpl(JmsMessageBroker kafkaMessageBroker) {
		super();
		this.kafkaMessageBroker = kafkaMessageBroker;
	}

	@Override
	public Long addListener(DatastoreKey observedObject_, 
			Class observedObjectClass_, 
			NotificationType notificationType_,
			String filterString_, 
			PersistedListener listener_) {
		// TODO Add listener datastore implementation 
		return null;
	}

	@Override
	public void notifyListeners(DatastoreKey observedObject_, 
			Class observedObjectClass_, 
			NotificationType notificationType_,
			String filterString_, 
			ListenerCallUserParameters parameters_) {
		kafkaMessageBroker.notifyListeners(observedObject_, observedObjectClass_, notificationType_, filterString_, parameters_);
		
		// TODO: Add listener notification implementation 
		
	}

	@Override
	public void removeAllListeners(DatastoreKey observedObjectKey_, 
			Class observedObjectClass_) {
		// TODO Add listener datastore implementation
		
	}

	@Override
	public void removeListener(Long listenerKey_) {
		// TODO Add listener datastore implementation
		
	}
	
	
}
