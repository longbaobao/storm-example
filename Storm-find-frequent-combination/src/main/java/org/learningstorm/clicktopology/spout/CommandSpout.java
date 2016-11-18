package org.learningstorm.clicktopology.spout;

import java.util.Map;

import org.apache.log4j.Logger;
import org.learningstorm.clicktopology.common.FieldNames;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CommandSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3757047085011759927L;

	public static Logger LOG = Logger.getLogger(CommandSpout.class);
	
	private SpoutOutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, 
			TopologyContext topologyContext, 
			SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
	}

	@Override
	public void nextTuple() {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.err.println("****************---------------------=====================");
		collector.emit(new Values("statistic"));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.COMMAND
		));
	}

}
