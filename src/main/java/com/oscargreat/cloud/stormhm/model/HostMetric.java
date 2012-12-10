package com.oscargreat.cloud.stormhm.model;

public class HostMetric {
	public HostMetric(long timeStamp, String performance) {
		super();
		this.setTimeStamp(timeStamp);
		this.performance = performance;
	}
	private long timeStamp;
	private String performance;
	public String getPerformance() {
		return performance;
	}
	public void setPerformance(String performance) {
		this.performance = performance;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
}
