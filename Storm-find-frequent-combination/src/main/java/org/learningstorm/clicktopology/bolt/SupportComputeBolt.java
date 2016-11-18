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

public class SupportComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private Map<ItemPair, Integer> pairCounts;
	int pairTotalCount;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		pairCounts = new HashMap<>();
		pairTotalCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		if ( tuple.getFields().get(0).equals(FieldNames.TOTAL_COUNT) ) {
			pairTotalCount = tuple.getIntegerByField(FieldNames.TOTAL_COUNT);
		}
		else if ( tuple.getFields().size() == 3 ) {
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			int pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		}
		else if ( tuple.getFields().get(0).equals(FieldNames.COMMAND) ) {
			for ( ItemPair itemPair : pairCounts.keySet() ) {
				double itemSupport = (double)(pairCounts.get(itemPair).intValue()) / pairTotalCount;
				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSupport));
			}
		}
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.SUPPORT
		));
	}
}
