package com.imarcats.microservice.order.management.order;

public class OrderActionMessage {
	private Long _orderKey;
	private OrderAction _orderAction;
	// this field is information only
	private String _marketCode;
	// TODO: Populate 
	private long _version;
	// only for cancel 
	private String _cancellationCommentLanguageKey;
	
	public Long getOrderKey() {
		return _orderKey;
	}
	public void setOrderKey(Long orderKey_) {
		_orderKey = orderKey_;
	}
	public OrderAction getOrderAction() {
		return _orderAction;
	}
	public void setOrderAction(OrderAction orderAction_) {
		_orderAction = orderAction_;
	}
	public String getMarketCode() {
		return _marketCode;
	}
	public void setMarketCode(String marketCode_) {
		_marketCode = marketCode_;
	}
	public long getVersion() {
		return _version;
	}
	public void setVersion(long version_) {
		_version = version_;
	}
	public String getCancellationCommentLanguageKey() {
		return _cancellationCommentLanguageKey;
	}
	public void setCancellationCommentLanguageKey(
			String cancellationCommentLanguageKey_) {
		_cancellationCommentLanguageKey = cancellationCommentLanguageKey_;
	}
}
