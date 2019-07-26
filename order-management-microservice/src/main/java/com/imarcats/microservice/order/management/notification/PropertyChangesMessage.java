package com.imarcats.microservice.order.management.notification;

import java.util.Date;
import java.util.List;

import com.imarcats.interfaces.client.v100.dto.types.PropertyChangeDto;
import com.imarcats.interfaces.client.v100.notification.ChangeOrigin;
import com.imarcats.interfaces.client.v100.notification.ObjectVersion;
import com.imarcats.interfaces.client.v100.notification.PropertyChanges;

public class PropertyChangesMessage {	
	private DatastoreKeyValue objectBeingChanged;
	private DatastoreKeyValue parentObject;	
	private String classBeingChanged;
	private List<PropertyValueChangeMessageField> changes;
	private long changeTimestamp;
	private ObjectVersion objectVersion;
	private String objectOwner;	
	private ChangeOrigin changeOrigin;
	
	public DatastoreKeyValue getObjectBeingChanged() {
		return objectBeingChanged;
	}
	public void setObjectBeingChanged(DatastoreKeyValue objectBeingChanged) {
		this.objectBeingChanged = objectBeingChanged;
	}
	public DatastoreKeyValue getParentObject() {
		return parentObject;
	}
	public void setParentObject(DatastoreKeyValue parentObject) {
		this.parentObject = parentObject;
	}
	public String getClassBeingChanged() {
		return classBeingChanged;
	}
	public void setClassBeingChanged(String classBeingChanged) {
		this.classBeingChanged = classBeingChanged;
	}
	public List<PropertyValueChangeMessageField> getChanges() {
		return changes;
	}
	public void setChanges(List<PropertyValueChangeMessageField> changes) {
		this.changes = changes;
	}
	public long getChangeTimestamp() {
		return changeTimestamp;
	}
	public void setChangeTimestamp(long changeTimestamp) {
		this.changeTimestamp = changeTimestamp;
	}
	public ObjectVersion getObjectVersion() {
		return objectVersion;
	}
	public void setObjectVersion(ObjectVersion objectVersion) {
		this.objectVersion = objectVersion;
	}
	public String getObjectOwner() {
		return objectOwner;
	}
	public void setObjectOwner(String objectOwner) {
		this.objectOwner = objectOwner;
	}
	public ChangeOrigin getChangeOrigin() {
		return changeOrigin;
	}
	public void setChangeOrigin(ChangeOrigin changeOrigin) {
		this.changeOrigin = changeOrigin;
	}

	public PropertyChanges createPropertyChanges() {
		PropertyChangeDto[] changesDto = new PropertyChangeDto[changes.size()];
		for (int i = 0; i < changesDto.length; i++) {
			changesDto[i] = changes.get(i).createDto();
		}
		try {
			return new PropertyChanges(
					objectBeingChanged.createDto(), 
					Class.forName(classBeingChanged), 
					parentObject != null ? parentObject.createDto() : null, 
					changesDto, new Date(), objectVersion, objectOwner, changeOrigin);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
}
