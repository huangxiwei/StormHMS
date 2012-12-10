package com.oscargreat.cloud.topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;

import com.oscargreat.cloud.stormhm.mq.IRPCCallee;
import com.oscargreat.cloud.stormhm.mq.MQClient;

/***
 * this spout pick host queries from outside MQ, create jobs and emit them. <p>
 * A query should be a list of host URL separated by ";". The spout will generate a UUID for the request
 * split the list and emit (id, host)
 * 
 * 
 * @author Oscar
 * 
 */
public abstract class MQListenerSpout<T> extends BaseRichSpout implements IRPCCallee{
	private static final long serialVersionUID = -6150238306896718594L;
	private String cmd = "CMD_DEFAULT";
	public static Logger LOG = Logger.getLogger(MQListenerSpout.class);
	boolean _isDistributed;
	private SpoutOutputCollector _collector;

	final private Queue<T> internalQueue = new LinkedList<T>();
	private MQClient mqclient;

	public MQListenerSpout(String cmd) {
		this(true);
		this.cmd = cmd;
	}
	protected Queue<T> getInternalQueue(){
		return internalQueue;
	}
	public MQListenerSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		mqclient = new MQClient();
		_collector = collector;
		try {
			mqclient.connect(cmd);
			
			mqclient.startWaitCall(cmd, this);
		} catch (IOException e) {
			LOG.error("cannot connect to MQ Server.");
			e.printStackTrace();
		}

	}
	
	public void close() {
		try {
			mqclient.stopWaitCall();
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
	public SpoutOutputCollector getCollector() {
		return _collector;
	}
}
