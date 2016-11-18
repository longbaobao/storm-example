package org.learningstorm.tfidf;

import org.learningstorm.tfidf.common.Conf;
import org.learningstorm.tfidf.functions.BatchCombiner;
import org.learningstorm.tfidf.functions.DocumentFetchFunction;
import org.learningstorm.tfidf.functions.DocumentTokenizer;
import org.learningstorm.tfidf.functions.PersistDocumentFunction;
import org.learningstorm.tfidf.functions.SplitAndProjectToFields;
import org.learningstorm.tfidf.functions.TermFilter;
import org.learningstorm.tfidf.functions.TfidfExpression;
import org.learningstorm.tfidf.state.TimeBasedRowStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import trident.cassandra.CassandraBucketState;

public class TermTopology {
	@SuppressWarnings("unused")
	private static Logger log = LoggerFactory.getLogger(TermTopology.class);
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static StateFactory getStateFactory(String rowKey) {
		CassandraBucketState.BucketOptions options = 
				new CassandraBucketState.BucketOptions();
		
		options.keyspace = "storm";
		options.columnFamily = "tfidf";
		options.rowKey = rowKey;
		options.keyStrategy = new TimeBasedRowStrategy();
		
		return CassandraBucketState.nonTransactional("localhost", options);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static StateFactory getBatchStateFactory(String rowKey) {
		CassandraBucketState.BucketOptions options = 
				new CassandraBucketState.BucketOptions();
		
		options.keyspace = "storm";
		options.columnFamily = "tfidfbatch";
		options.rowKey = rowKey;
		
		return CassandraBucketState.nonTransactional("localhost", options);
	}
	
	public static class PrintlnFunction extends BaseFunction {
		private static final long serialVersionUID = 4693642741476534029L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			System.out.println("New Tuple for printing: " + tuple.toString());
			collector.emit(new Values("dummy"));
		}
	}
	
	public static class StaticSourceFunction extends BaseFunction {
		private static final long serialVersionUID = -6833114156466610210L;
		private String source;
		
		public StaticSourceFunction(String source) {
			this.source = source;
		}
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			collector.emit(new Values(source));
		}
	}

	@SuppressWarnings("unchecked")
	private static Stream getUrlStream(TridentTopology topology) {
		FixedBatchSpout testSpout = new FixedBatchSpout(
				new Fields("url"), 1,
				new Values("/home/stormdev/Document/article/1,txt"),
				new Values("/home/stormdev/Document/article/2.txt"));
		testSpout.setCycle(true);
		
		Stream documentStream = topology
				.newStream("documentSpout", testSpout)
				.parallelismHint(16);
		return documentStream;
	}

	private static void addDQueryStream(LocalDRPC drpc,
			TridentTopology topology, TridentState state) {
		topology.newDRPCStream("dQuery", drpc)
			.each(new Fields("args"), new Split(), new Fields("source"))
			.stateQuery(state, new Fields("source"), new MapGet(), new Fields("d"))
			.each(new Fields("d"), new FilterNull())
			.project(new Fields("source", "d"));
	}

	private static void addDFQueryStream(LocalDRPC drpc,
			TridentTopology topology, TridentState state) {
		topology.newDRPCStream("dfQuery", drpc)
		.each(new Fields("args"), new Split(), new Fields("term"))
		.stateQuery(state, new Fields("term"), new MapGet(), new Fields("df"))
		.each(new Fields("df"), new FilterNull())
		.project(new Fields("term", "df"));
	}
	
	private static void addTFIDFQueryStream(TridentState tfState,
			TridentState dfState,
			TridentState dState,
			TridentTopology topology, LocalDRPC drpc) {
		TridentState batchDfState = topology.newStaticState(getBatchStateFactory("df"));
		TridentState batchDState = topology.newStaticState(getBatchStateFactory("d"));
		TridentState batchTfState = topology.newStaticState(getBatchStateFactory("tf"));
		
		topology.newDRPCStream("tfidfQuery", drpc)
				.each(new Fields("args"), new SplitAndProjectToFields(), 
						new Fields("documentId", "term"))
				.each(new Fields(), new StaticSourceFunction("twitter"), 
						new Fields("source"))
				.stateQuery(tfState, new Fields("documentId", "term"), 
						new MapGet(), new Fields("tf_rt"))
				.stateQuery(dfState, new Fields("term"), 
						new MapGet(), new Fields("df_rt"))
				.stateQuery(dState, new Fields("source"), 
						new MapGet(), new Fields("d_rt"))
				.stateQuery(batchTfState, new Fields("documentId", "term"), 
						new MapGet(), new Fields("tf_batch"))
				.stateQuery(batchDfState, new Fields("term"), 
						new MapGet(), new Fields("df_batch"))
				.stateQuery(batchDState, new Fields("source"), 
						new MapGet(), new Fields("d_batch"))
				.each(new Fields("tf_rt", "df_rt", "d_rt",
						"tf_batch", "df_batch", "d_batch"),
						new BatchCombiner(), new Fields("tf", "d", "df"))
				.each(new Fields("term", "documentId", "tf", "d", "df"),
						new TfidfExpression(), new Fields("tfidf"))
				.each(new Fields("tfidf"), new FilterNull())
				.project(new Fields("documentId", "term", "tfidf"));
	}
	
	@SuppressWarnings({ "rawtypes" })
	public static TridentTopology buildTopology(ITridentSpout spout,
			LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();
		
		Stream documentStream = getUrlStream(topology)
				.each(new Fields("url"),
						new DocumentFetchFunction(),
						new Fields("document", "documentId", "source"));
		
		documentStream.each(new Fields("documentId", "document"),
				new PersistDocumentFunction(), new Fields());
		
		Stream termStream = documentStream
				.parallelismHint(20)
				.each(new Fields("document"),
						new DocumentTokenizer(),
						new Fields("dirtyTerm"))
				.each(new Fields("dirtyTerm"), 
						new TermFilter(),
						new Fields("term"))
				.project(new Fields("term", "documentId", "source"));
		
		TridentState dState = termStream.groupBy(
				new Fields("source")).persistentAggregate(
						getStateFactory("d"),
						new Count(),
						new Fields("d"));
		
		addDQueryStream(drpc, topology, dState);
		
		TridentState dfState = termStream.groupBy(
				new Fields("term")).persistentAggregate(
						getStateFactory("df"),
						new Count(),
						new Fields("df"));
		
		addDFQueryStream(drpc, topology, dfState);
		
		TridentState tfState = termStream.groupBy(
				new Fields("documentId", "term")).persistentAggregate(
						getStateFactory("tf"),
						new Count(),
						new Fields("tf"));
		
		addTFIDFQueryStream(tfState, dfState, dState, topology, drpc);
		
		return topology;
	}
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		conf.put(Conf.REDIS_PORT_KEY, Conf.REDIS_DEFAULT_JEDIS_PORT);
		conf.put("DOCUMENT_PATH", "document.avro");
		
		if ( args.length == 0 ) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			conf.setDebug(true);
			TridentTopology topology = buildTopology(null, drpc);
			
			cluster.submitTopology("tfidf", conf, topology.build());
			Thread.sleep(60000);
		}
		else {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
	}
}
