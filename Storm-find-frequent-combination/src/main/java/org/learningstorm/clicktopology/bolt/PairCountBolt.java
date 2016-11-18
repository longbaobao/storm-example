package org.learningstorm.clicktopology.bolt;

import java.util.HashMap;
import java.util.Map;

import org.learningstorm.clicktopology.common.FieldNames;
import org.learningstorm.clicktopology.common.ItemPair;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PairCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private Map<ItemPair, Integer> pairCounts;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		pairCounts = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String item1 = tuple.getStringByField(FieldNames.ITEM1);
		String item2 = tuple.getStringByField(FieldNames.ITEM2);
		
		ItemPair itemPair = new ItemPair(item1, item2);
		int pairCount = 0;
		if ( pairCounts.containsKey(itemPair) ) {
			pairCount = pairCounts.get(itemPair);
		}
		
		pairCount ++;
		pairCounts.put(itemPair, pairCount);
		
		collector.emit(new Values(item1, item2, pairCount));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.PAIR_COUNT
		));
	}
}
