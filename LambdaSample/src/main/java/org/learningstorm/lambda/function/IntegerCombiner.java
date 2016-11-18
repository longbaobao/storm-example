package org.learningstorm.lambda.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class IntegerCombiner extends BaseFunction {
	private static final long serialVersionUID = 3939565604571345323L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		int number1 = tuple.getInteger(0);
		int number2 = tuple.getInteger(1);
		
		collector.emit(new Values(number1 + number2));
	}

}
