package org.learningstorm.lambda;

import org.learningstorm.lambda.function.CassandraPersistent;
import org.learningstorm.lambda.function.CityOfProvinceRate;
import org.learningstorm.lambda.function.IntegerCombiner;
import org.learningstorm.lambda.function.IntegerCounter;
import org.learningstorm.lambda.function.NullToZero;
import org.learningstorm.lambda.function.SplitProvinceCity;
import org.learningstorm.lambda.mapper.ProvinceBatchMapper;
import org.learningstorm.lambda.mapper.ProvinceCityBatchMapper;
import org.learningstorm.lambda.mapper.ProvinceCityRTMapper;
import org.learningstorm.lambda.mapper.ProvinceRTMapper;
import org.learningstorm.lambda.spout.UserInfoSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class ProvinceCityRateTopology {
	private static final Logger LOG = LoggerFactory.getLogger(ProvinceCityRateTopology.class);
	
	public static StormTopology buildTopology(LocalDRPC drpc) {
		LOG.info("Building topology");
		
		TridentTopology topology = new TridentTopology();
		UserInfoSpout spout1 = new UserInfoSpout(4);
		
		Stream userInfoStream = topology.newStream("spout1", spout1)
				.each(new Fields("name", "telephone", "email", "age", "province", "city"),
						new CassandraPersistent());
		
		TridentState provinceCityCountRTState = userInfoStream
				.groupBy(new Fields("province", "city"))
				.persistentAggregate(CassandraCqlMapState.nonTransactional(
						new ProvinceCityRTMapper()), 
						new IntegerCounter(), 
						new Fields("count"))
				.parallelismHint(6);
		
		TridentState provinceCountRTState = userInfoStream
				.groupBy(new Fields("province"))
				.persistentAggregate(CassandraCqlMapState.nonTransactional(
						new ProvinceRTMapper()), 
						new IntegerCounter(), 
						new Fields("count"))
				.parallelismHint(6);	
		
		TridentState provinceCityCountBatchState = topology.newStaticState(
				CassandraCqlMapState.nonTransactional(new ProvinceCityBatchMapper()));
		
		TridentState provinceCountBatchState = topology.newStaticState(
				CassandraCqlMapState.nonTransactional(new ProvinceBatchMapper()));
		
		topology.newDRPCStream("provinceCity", drpc)
			.each(new Fields("args"), new SplitProvinceCity(),
					new Fields("province", "city"))
					
			.stateQuery(provinceCountRTState, new Fields("province"), 
					new MapGet(), new Fields("provinceRTCount"))
			.each(new Fields("provinceRTCount"), new FilterNull())
			
			.stateQuery(provinceCityCountRTState, new Fields("province", "city"), 
					new MapGet(), new Fields("cityRTCount"))
			.each(new Fields("cityRTCount"), new FilterNull())
			
			.stateQuery(provinceCountBatchState, new Fields("province"), 
					new MapGet(), new Fields("provinceBatchCount_null"))
			.each(new Fields("provinceBatchCount_null"), new NullToZero(), new Fields("provinceBatchCount"))
			
			.stateQuery(provinceCityCountBatchState, new Fields("province", "city"), 
					new MapGet(), new Fields("cityBatchCount_null"))
			.each(new Fields("cityBatchCount_null"), new NullToZero(), new Fields("cityBatchCount"))
			
			.each(new Fields("provinceRTCount", "provinceBatchCount"), 
					new IntegerCombiner(), new Fields("provinceCount"))
					
			.each(new Fields("cityRTCount", "cityBatchCount"), 
					new IntegerCombiner(), new Fields("cityCount"))
					
			.each(new Fields("provinceCount", "cityCount"), 
					new CityOfProvinceRate(), new Fields("rate"))
			.project(new Fields("province", "city", "rate"));
		
		return topology.build();
	}
	
	public static void main(String[] args) throws InterruptedException {
		final Config config = new Config();
		config.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
		final LocalCluster cluster = new LocalCluster();
		LocalDRPC client = new LocalDRPC();
		
		LOG.info("Submitting topology.");
		cluster.submitTopology("lambdasample", config, 
				buildTopology(client));
		LOG.info("Topology submitted");
		Thread.sleep(10000);
		
		LOG.info("DRPC Query: Find province and city count [AH Hefei]: {}",
				client.execute("provinceCity", "AH Hefei"));
		
		cluster.shutdown();
		client.shutdown();
	}
}
