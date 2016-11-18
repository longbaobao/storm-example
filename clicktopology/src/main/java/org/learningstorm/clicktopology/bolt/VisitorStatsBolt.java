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

public class VisitorStatsBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private int total = 0;
	private int uniqueCount = 0;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		boolean unique = Boolean.parseBoolean(
				tuple.getStringByField(FieldNames.UNIQUE));
		
		if ( unique ) {
			uniqueCount ++;
		}
		collector.emit(new Values(total, uniqueCount));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.TOTAL_COUNT,
				FieldNames.TOTAL_UNIQUE
		));
	}

}
