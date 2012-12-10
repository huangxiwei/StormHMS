package com.oscargreat.cloud.teststorm;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oscargreat.cloud.stormhm.model.CheckReply;
import com.oscargreat.cloud.stormhm.mq.MQClient;


/***
 * Proxy to Monitoring System implemented on Twitter Storm
 * @author Oscar
 *
 */
public class TestAgent {
	private MQClient qclient;
	private MQClient cclient;
	
	
	public TestAgent() {
		qclient = new MQClient();
		cclient = new MQClient();
		
		
	}
	

	public void init() throws IOException {
		qclient.connect("query");

		cclient.connect("check");

	}

	public void close() throws IOException {
		qclient.disconnect();
		cclient.disconnect();
		
	}
	

	public String startQuery(List<String> hostURLs) {
		StringBuilder sb = new StringBuilder();
		for(String s: hostURLs){
			sb.append(s);
			sb.append(";");
		}
		try {
			String ret = qclient.syncCall(sb.toString());
			return ret;
		} catch (Exception e) {//silent fail
			return "";
		}
		
	}

	public CheckReply checkProgress(String queryID) {
		try {
			String ret = cclient.syncCall(queryID);
			ObjectMapper map = new ObjectMapper();
			CheckReply remote_result = map.readValue(ret, CheckReply.class);
			return remote_result;
		} catch (Exception e) {//silent fail
			return null;
		}
	}


}
