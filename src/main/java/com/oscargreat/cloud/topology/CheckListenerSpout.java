package com.oscargreat.cloud.topology;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.oscargreat.cloud.stormhm.mq.CallbackContext;
import com.oscargreat.cloud.stormhm.mq.MQClient;
import com.oscargreat.cloud.stormhm.wrapper.Out;

public class CheckListenerSpout extends MQListenerSpout<CallbackContext>{

	private static final String CMD = "check";

	public CheckListenerSpout() {
		super(CMD);
		// TODO Auto-generated constructor stub
	}

	private static final long serialVersionUID = -3540262256108442308L;

	@Override
	public void remotedCalled(MQClient client, CallbackContext context) {
		getInternalQueue().offer(context);
	}

	@Override
	public void nextTuple() {
		while(!getInternalQueue().isEmpty()){
			CallbackContext c = getInternalQueue().poll();
			String jobid = c.getMessage();
			Out.print("start dealing check: %s\n", jobid);
			getCollector().emit(new Values(jobid, 1, c));
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jobid", "checkflag", "callback"));
	}

}
