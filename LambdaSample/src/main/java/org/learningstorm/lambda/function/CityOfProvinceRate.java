package org.learningstorm.lambda.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CityOfProvinceRate extends BaseFunction {
	private static final long serialVersionUID = -6584323287157113532L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		int provinceCount = tuple.getInteger(0);
		int cityCount = tuple.getInteger(1);
		
		double rate = (double)cityCount / provinceCount;
		
		collector.emit(new Values(rate));
	}

}
