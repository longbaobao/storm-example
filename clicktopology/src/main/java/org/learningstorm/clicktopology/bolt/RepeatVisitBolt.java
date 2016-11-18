package org.learningstorm.clicktopology.bolt;

import java.util.Map;

import org.learningstorm.clicktopology.common.ConfKeys;
import org.learningstorm.clicktopology.common.FieldNames;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RepeatVisitBolt extends BaseRichBolt {

	private static final long serialVersionUID = -283193209123063833L;
	
	private OutputCollector collector;
	private Jedis jedis;
	private String host;
	private int port;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		host = conf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.valueOf(
				conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void execute(Tuple tuple) {
		String clientKey = tuple.getStringByField(FieldNames.CLIENT_KEY);
		String url = tuple.getStringByField(FieldNames.URL);
		String key = url + ":" + clientKey;
		String value = jedis.get(key);
		
		if ( value == null ) {
			jedis.set(key, "visited");
			collector.emit(new Values(
					clientKey,
					url,
					Boolean.TRUE.toString()
			));
		}
		else {
			collector.emit(new Values(
					clientKey,
					url,
					Boolean.FALSE.toString()
			));
		}
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.CLIENT_KEY,
				FieldNames.URL,
				FieldNames.UNIQUE
		));
	}

}
