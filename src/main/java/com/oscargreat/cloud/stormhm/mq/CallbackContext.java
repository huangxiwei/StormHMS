package com.oscargreat.cloud.stormhm.mq;

public class CallbackContext {
	public CallbackContext(String replyQueue, String callID, long deliveryTag, String message) {
		super();
		this.callID = callID;
		this.replyQueue = replyQueue;
		this.deliveryTag = deliveryTag;
		this.message = message;
	}

	private  String callID;
	private String replyQueue;
	private long deliveryTag;
	private String message;

	public String getCallID() {
		return callID;
	}

	public void setCallID(String callID) {
		this.callID = callID;
	}

	public String getReplyQueue() {
		return replyQueue;
	}

	public void setReplyQueue(String replyQueue) {
		this.replyQueue = replyQueue;
	}

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
