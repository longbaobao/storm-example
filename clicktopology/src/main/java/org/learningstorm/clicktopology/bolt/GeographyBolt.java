package org.learningstorm.clicktopology.bolt;

import java.util.Map;

import org.json.simple.JSONObject;
import org.learningstorm.clicktopology.common.FieldNames;
import org.learningstorm.clicktopology.common.IPResolver;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeographyBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5413696588647790366L;

	private IPResolver resolver;
	private OutputCollector collector;
	
	public GeographyBolt(IPResolver resolver) {
		this.resolver = resolver;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		String ip = tuple.getStringByField(FieldNames.IP);
		JSONObject json = resolver.resolveIP(ip);

		String city = (String)json.get(FieldNames.CITY);
		String country = (String)json.get(FieldNames.COUNTRY_NAME);
		collector.emit(new Values(country, city));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.COUNTRY,
				FieldNames.CITY
		));
	}

}
