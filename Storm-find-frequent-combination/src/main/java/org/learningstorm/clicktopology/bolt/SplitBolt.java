package org.learningstorm.clicktopology.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.learningstorm.clicktopology.common.FieldNames;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private Map<String, List<String>> orderItems;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		orderItems = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String id = tuple.getStringByField(FieldNames.ID);
		String newItem = tuple.getStringByField(FieldNames.NAME);
		
		if ( !orderItems.containsKey(id) ) {
			ArrayList<String> items = new ArrayList<String>();
			items.add(newItem);
			
			orderItems.put(id, items);
			return;
		}
		
		List<String> items = orderItems.get(id);
		for ( String existItem : items ) {
			collector.emit(createPair(newItem, existItem));
		}
		items.add(newItem);
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2
		));
	}

	private Values createPair(String item1, String item2) {
		if ( item1.compareTo(item2) > 0 ) {
			return new Values(item1, item2);
		}
		
		return new Values(item2, item1);
	}
}
