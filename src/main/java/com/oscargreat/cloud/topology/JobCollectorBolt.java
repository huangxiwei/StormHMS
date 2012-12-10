package com.oscargreat.cloud.topology;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oscargreat.cloud.stormhm.model.CheckReply;
import com.oscargreat.cloud.stormhm.model.HostInfo;
import com.oscargreat.cloud.stormhm.model.HostMetric;
import com.oscargreat.cloud.stormhm.mq.CallbackContext;
import com.oscargreat.cloud.stormhm.wrapper.IMemcache;
import com.oscargreat.cloud.stormhm.wrapper.LocalMemcache;
import com.oscargreat.cloud.stormhm.wrapper.Out;

public class JobCollectorBolt extends MQReplyBolt{
	private static final String QUEUE_NAME = "QUEUE_CHECKPROGRESS";
	public static Logger LOG = Logger.getLogger(JobCollectorBolt.class);
	OutputCollector collector;
	public JobCollectorBolt(boolean isDistributed) {
		super(QUEUE_NAME, isDistributed);
		// TODO Auto-generated constructor stub
	}

	private static final long serialVersionUID = 8625920690589352337L;
	IMemcache cache ;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf,context,collector);
		cache = new LocalMemcache();
		cache.init();
		this.collector = collector;
	}
	@Override
	public void execute(Tuple tuple) {
		Out.print("jcollect: new tuple");
		if(!tuple.contains("checkflag")){
			doCache(tuple);
		}
		else
			doReport(tuple, collector);
		
	}

	private void doReport(Tuple tuple, OutputCollector collector) {
		
		Out.print("jcollect: do report");
		
		String jobid = tuple.getStringByField("jobid");
		Object object = tuple.getValueByField("callback");
		if(!(object instanceof CallbackContext))
			return ;
		CallbackContext callback= (CallbackContext)object;
		
		if(jobid == null)
			return;
		Object obj = cache.get(jobid);
		HostInfo info;
		if(obj == null || !(obj instanceof HostInfo)){
			info = new HostInfo();
			info.setJobid("");
			info.setHostTotalNum(1);
			
		}
		else 
			info = (HostInfo) obj;
		
		
		if(info.getMetricMap().size()==info.getHostTotalNum()){
			int progress = 100;
			reportToMQ(callback, progress,info);
		}
		else{
			int progress = (info.getMetricMap().size() *1000)/info.getHostTotalNum()/10;
			reportToMQ(callback,progress,null);
		}
			
			
			
	}

	private void reportToMQ(CallbackContext callback, int progress, HostInfo info) {
		ObjectMapper map =new ObjectMapper();
		CheckReply reply = new CheckReply(progress, info);
		String replystr;
		try {
			replystr = map.writeValueAsString(reply);
		} catch (JsonProcessingException e) {
			replystr = "{progress = 0}";
		}
		try {
			this.sendMessage(callback,replystr);
		} catch (IOException e) {
			LOG.error("cannot write to MQ Server");
		}
		
	}

	private void doCache(Tuple tuple) {
		String jobid = tuple.getStringByField("jobid");
		String hosturl = tuple.getStringByField("host");
		int hostnum = tuple.getIntegerByField("hostnum");
		String perf = tuple.getStringByField("perf");
		long timestamp = tuple.getLongByField("timestamp");
		
		Object obj = cache.get(jobid);
		HostInfo info;
		if(obj == null || !(obj instanceof HostInfo)){
			info = new HostInfo();
			info.setJobid(jobid);
			info.setHostTotalNum(hostnum);
			
		}
		else 
			info = (HostInfo) obj;
		
		info.getMetricMap().put(hosturl, new HostMetric(timestamp, perf));
		Out.print("start cache job: %s of %s\n", hosturl, jobid);
		cache.set(jobid, info);
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}