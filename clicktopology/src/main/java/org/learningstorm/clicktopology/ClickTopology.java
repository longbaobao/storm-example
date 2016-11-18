package org.learningstorm.clicktopology;

import org.learningstorm.clicktopology.bolt.GeoStatsBolt;
import org.learningstorm.clicktopology.bolt.GeographyBolt;
import org.learningstorm.clicktopology.bolt.RepeatVisitBolt;
import org.learningstorm.clicktopology.bolt.VisitorStatsBolt;
import org.learningstorm.clicktopology.common.ConfKeys;
import org.learningstorm.clicktopology.common.FieldNames;
import org.learningstorm.clicktopology.common.HttpIpResolver;
import org.learningstorm.clicktopology.spout.ClickSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class ClickTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		ClickTopology topology = new ClickTopology();
		
		if ( args != null && args.length > 1 ) {
			topology.runCluster(args[0], args[1]);
		}
		else {
			System.out.println(
					"Running in local mode" +
					"\nRedis ip missing for cluster run"
			);
			topology.runLocal(10000);
		}
	}

	public ClickTopology() {
		builder.setSpout("clickSpout", new ClickSpout(), 10);
		builder.setBolt("repeatBolt", new RepeatVisitBolt(), 10)
			.shuffleGrouping("clickSpout");
		builder.setBolt("geographyBolt", 
				new GeographyBolt(new HttpIpResolver()), 10)
				.shuffleGrouping("clickSpout");
		builder.setBolt("totalStats", new VisitorStatsBolt(), 1)
			.globalGrouping("repeatBolt");
		
		builder.setBolt("geoStats", new GeoStatsBolt(), 10)
			.fieldsGrouping("geographyBolt", 
					new Fields(FieldNames.COUNTRY));
	}
	
	public TopologyBuilder getBuilder() {
		return builder;
	}

	public LocalCluster getLocalCluster() {
		return cluster;
	}
	
	private void runLocal(int runTime) {
		conf.setDebug(true);
		conf.put(ConfKeys.REDIS_HOST, "localhost");
		conf.put(ConfKeys.REDIS_PORT, "6379");
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, 
				builder.createTopology());
		
		if ( runTime > 0 ) {
			Utils.sleep(runTime);
			shutdownLocal();
		}
	}

	private void shutdownLocal() {
		if ( cluster != null ) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	private void runCluster(String name, String redisHost) throws AlreadyAliveException, InvalidTopologyException {
		conf.setNumWorkers(20);
		conf.put(ConfKeys.REDIS_HOST, redisHost);
		conf.put(ConfKeys.REDIS_PORT, "6379");
		StormSubmitter.submitTopology(name, conf, 
				builder.createTopology());
	}

}
