package org.learningstorm.lambda.function;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class IntegerCounter implements CombinerAggregator<Integer> {
	private static final long serialVersionUID = 1939834042096819669L;

	@Override
	public Integer init(TridentTuple tuple) {
		return 1;
	}

	@Override
	public Integer combine(Integer val1, Integer val2) {
		return val1 + val2;
	}

	@Override
	public Integer zero() {
		return 0;
	}

}
