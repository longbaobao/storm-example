package org.learningstorm.lambda.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitProvinceCity extends BaseFunction {
	private static final long serialVersionUID = -424682113591364498L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String[] args = tuple.getString(0).split(" ");
		
		collector.emit(new Values(args[0], args[1]));
	}

}
