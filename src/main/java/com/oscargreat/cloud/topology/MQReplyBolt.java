package com.oscargreat.cloud.topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

import com.oscargreat.cloud.stormhm.mq.CallbackContext;
import com.oscargreat.cloud.stormhm.mq.MQClient;

abstract public class MQReplyBolt  extends BaseRichBolt {
	private static final long serialVersionUID = -2814954321794255789L;

	private String cmd = "QUEUE_DEFAULT";
	public static Logger LOG = Logger.getLogger(MQReplyBolt.class);
	boolean _isDistributed;

	private MQClient mqclient;

	public MQReplyBolt(String cmd) {
		_isDistributed = true;
		
		this.cmd= cmd;
	}

	public MQReplyBolt(String queueName, boolean isDistributed) {
		this(queueName);
		_isDistributed = true;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
			mqclient = new MQClient();
			mqclient.connect(cmd);
		} catch (IOException e) {
			LOG.error("cannot connect to MQ Server.");
		}

	}

	protected void sendMessage(CallbackContext context, String message) throws IOException{
		//MQClient.reply(mqclient.context, message);
	}
	public void close() {
		try {
			mqclient.disconnect();
		} catch (IOException e) {
			LOG.error("error in close MQ");
		}
		
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

}
