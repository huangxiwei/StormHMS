package com.oscargreat.cloud.stormhm.model;

import java.util.HashMap;
import java.util.Map;

public class HostInfo {
	private String jobid;
	private int hostTotalNum;
	private Map<String,HostMetric> metricMap;
	
	public HostInfo() {
		metricMap = new HashMap<String, HostMetric>();
	}
	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public int getHostTotalNum() {
		return hostTotalNum;
	}
	public void setHostTotalNum(int hostTotalByn) {
		this.hostTotalNum = hostTotalByn;
	}
	public Map<String, HostMetric> getMetricMap() {
		return metricMap;
	}
	public void setMetricMap(Map<String, HostMetric> metricMap) {
		this.metricMap = metricMap;
	}
	
}
