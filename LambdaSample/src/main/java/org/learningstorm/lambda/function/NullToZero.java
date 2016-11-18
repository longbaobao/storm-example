package org.learningstorm.lambda.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class NullToZero extends BaseFunction {
	private static final long serialVersionUID = 320907789495486723L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Integer value = tuple.getInteger(0);
		if ( value == null ) {
			collector.emit(new Values(0));
		}
	}
}
