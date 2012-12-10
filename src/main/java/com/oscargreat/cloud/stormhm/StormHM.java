package com.oscargreat.cloud.stormhm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.oscargreat.cloud.topology.CheckListenerSpout;
import com.oscargreat.cloud.topology.JobCollectorBolt;
import com.oscargreat.cloud.topology.MetricsQueryBolt;
import com.oscargreat.cloud.topology.QueryListenerSpout;

/***
 * Storm HMS: a host monitoring system based Storm.
 * this is main program submiting topology to Storm. Current use LocalCluster for test purpose. 
 * @author Oscar
 *
 */
public class StormHM {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("qspout", new QueryListenerSpout(), 1);
		builder.setSpout("cspout", new CheckListenerSpout(), 1);

		builder.setBolt("mquery", new MetricsQueryBolt(), 8).fieldsGrouping(
				"qspout", new Fields("host"));
		builder.setBolt("jcollect", new JobCollectorBolt(false), 8)
				.fieldsGrouping("mquery", new Fields("jobid"))
				.fieldsGrouping("cspout", new Fields("jobid"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("host-monitor", conf,
					builder.createTopology());

			System.out.println("Storm started with Topology host-monitor"
					+ "\nHit enter to stop it...");
			System.in.read();
			cluster.shutdown();
		}

	}
}
