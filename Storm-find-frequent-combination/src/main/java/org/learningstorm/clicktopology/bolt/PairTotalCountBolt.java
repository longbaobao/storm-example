package org.learningstorm.clicktopology.bolt;

import java.util.Map;

import org.learningstorm.clicktopology.common.FieldNames;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PairTotalCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	int totalCount;
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		totalCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		totalCount ++;
		collector.emit(new Values(totalCount));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.TOTAL_COUNT
		));
	}
}
