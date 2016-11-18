package org.learningstorm.clicktopology.spout;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.learningstorm.clicktopology.common.ConfKeys;
import org.learningstorm.clicktopology.common.FieldNames;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OrderSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3757047085011759927L;

	public static Logger LOG = Logger.getLogger(OrderSpout.class);
	
	private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, 
			TopologyContext topologyContext, 
			SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		
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
	public void nextTuple() {
		String content = jedis.rpop("orders");
		
		if ( content == null || "nil".equals(content) ) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			JSONObject obj = (JSONObject) JSONValue.parse(content);
			String id = obj.get(FieldNames.ID).toString();
			JSONArray items = (JSONArray)obj.get(FieldNames.ITEMS);
			System.out.println(id);
			for ( Object itemObj : items ) {
				JSONObject item = (JSONObject)itemObj;
				String name = item.get(FieldNames.NAME).toString();
				int count = Integer.parseInt(item.get(FieldNames.COUNT).toString());
				collector.emit(new Values(id, name, count));
				if ( jedis.hexists("itemCounts", name) ) {
					jedis.hincrBy("itemCounts", name, 1);
				}
				else {
					jedis.hset("itemCounts", name, "1");
				}
			}
		}
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.ID,
				FieldNames.NAME,
				FieldNames.COUNT
		));
	}

}
