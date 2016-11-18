package org.learningstorm.clicktopology;

import org.learningstorm.clicktopology.bolt.ConfidenceComputeBolt;
import org.learningstorm.clicktopology.bolt.FilterBolt;
import org.learningstorm.clicktopology.bolt.PairCountBolt;
import org.learningstorm.clicktopology.bolt.PairTotalCountBolt;
import org.learningstorm.clicktopology.bolt.SplitBolt;
import org.learningstorm.clicktopology.bolt.SupportComputeBolt;
import org.learningstorm.clicktopology.common.ConfKeys;
import org.learningstorm.clicktopology.common.FieldNames;
import org.learningstorm.clicktopology.spout.CommandSpout;
import org.learningstorm.clicktopology.spout.OrderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class OrderTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		OrderTopology topology = new OrderTopology();
		
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

	public OrderTopology() {
		builder.setSpout("orderSpout", new OrderSpout(), 5);
		builder.setSpout("commandSpout", new CommandSpout(), 1);
		
		builder.setBolt("splitBolt", new SplitBolt(), 5).
			fieldsGrouping("orderSpout", new Fields(FieldNames.ID));
		
		builder.setBolt("pairCountBolt", new PairCountBolt(), 5).
			fieldsGrouping("splitBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
		builder.setBolt("pairTotalCountBolt", new PairTotalCountBolt(), 1).
			globalGrouping("splitBolt");

		builder.setBolt("supportComputeBolt", new SupportComputeBolt(), 5).
			allGrouping("pairTotalCountBolt").
			fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2)).
			allGrouping("commandSpout");
		builder.setBolt("confidenceComputeBolt", new ConfidenceComputeBolt(), 5).
			fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2)).
			allGrouping("commandSpout");
		
		builder.setBolt("filterBolt", new FilterBolt()).
			fieldsGrouping("supportComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2)).
			fieldsGrouping("confidenceComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
//		builder.setBolt("geographyBolt", 
//				new GeographyBolt(new HttpIpResolver()), 10)
//				.shuffleGrouping("clickSpout");
//		builder.setBolt("totalStats", new SplitBolt(), 1)
//			.globalGrouping("repeatBolt");
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
