package com.oscargreat.cloud.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.oscargreat.cloud.stormhm.mq.CallbackContext;
import com.oscargreat.cloud.stormhm.mq.MQClient;
import com.oscargreat.cloud.stormhm.wrapper.Out;

public class QueryListenerSpout extends MQListenerSpout<String>{

	private static final String CMD = "query";
	public QueryListenerSpout() {
		super(CMD);
	}

	private static final long serialVersionUID = 9122201547216450923L;
	@Override
	public void remotedCalled(MQClient c, CallbackContext context) {
		String id = UUID.randomUUID().toString();
		getInternalQueue().offer(id + "////" + context.getMessage());
		try {
			c.reply(context, id);
		} catch (IOException e) {
			LOG.error("cannot send msg to MQ Server.");
		}
		
	}
	
	public void nextTuple() {
		while(!getInternalQueue().isEmpty()){
			buildandSendJob(getInternalQueue().poll());
        }
	}
	/***
	 * Break Host List into a Query Job
	 * @param poll
	 * @return
	 */
	private void buildandSendJob(String hostlist) {
		
		Out.print("start dealing job: %s\n", hostlist);
		if(hostlist == null)
				return;
		String[] items = hostlist.split("////");
		if(items == null || items.length != 2){
			return;
		}
			
		String id = items[0];
		String[] hosts = items[1].trim().split(";");
		List<String> validhosts = new ArrayList<String>(hosts.length);
			
		for(String host:hosts)
			if(isValidURL(host)){
				validhosts.add(host);
			}
				
		int hostnum = validhosts.size();
		for(String host:validhosts)
			getCollector().emit(new Values(id,hostnum,host));
	}

	private boolean isValidURL(String host) {
		//TODO: add more check here
		return host != null && !host.equals("");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jobid", "hostnum", "host"));
	}

}
