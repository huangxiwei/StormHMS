package com.oscargreat.cloud.topology;

import java.sql.Timestamp;
import java.util.Map;

import com.oscargreat.cloud.stormhm.wrapper.MetricsDBMock;
import com.oscargreat.cloud.stormhm.wrapper.MetricsDao;
import com.oscargreat.cloud.stormhm.wrapper.MetricsQueryClient;
import com.oscargreat.cloud.stormhm.wrapper.Out;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MetricsQueryBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -835844927870942142L;
	private static final int HOST_QUERY_MAX_RETRY_TIMES = 3;
	private static final long HOST_QUERY_INTERVAL_IN_MS = 5000;
	MetricsQueryClient client;
	MetricsDao metricDAO;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		client = new MetricsQueryClient();
		metricDAO= new MetricsDBMock();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String jobid = tuple.getStringByField("jobid");
		String hosturl = tuple.getStringByField("host");
		int hostnum = tuple.getIntegerByField("hostnum");
		String perf = "";
		long timestamp = 0;
		Out.print("start get metric : %s of %s\n", hosturl, jobid);
		for (int i = 0; i < HOST_QUERY_MAX_RETRY_TIMES; i++) {
			
			perf = client.fetchInfo(hosturl);
			timestamp = System.currentTimeMillis();
			if (perf != null)
				break;
			try {
				Thread.sleep(HOST_QUERY_INTERVAL_IN_MS);
			} catch (InterruptedException e) {
			}
		}
		
		metricDAO.storeMetrics(perf, new Timestamp(timestamp));
		collector.emit(new Values(jobid, hostnum, hosturl, perf, timestamp));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jobid", "hostnum", "host", "perf", "timestamp"));
	}
}
