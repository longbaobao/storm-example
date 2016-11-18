package org.learningstorm.tfidf.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class BatchCombiner extends BaseFunction {
	private static final long serialVersionUID = -5785169864071455111L;
	private static Logger log = LoggerFactory.getLogger(BatchCombiner.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d_rt = (double)tuple.getLongByField("d_rt");
			double df_rt = (double)tuple.getLongByField("df_rt");
			double tf_rt = (double)tuple.getLongByField("tf_rt");
	
			double d_batch = (double)tuple.getLongByField("d_batch");
			double df_batch = (double)tuple.getLongByField("df_batch");
			double tf_batch = (double)tuple.getLongByField("tf_batch");
			
			log.debug(String.format("Combining! d_rt=%d "
					+ "df_rt=%d "
					+ "tf_rt=%d "
					+ "d_batch=%d "
					+ "df_batch=%d "
					+ "tf_batch=%d",
					d_rt, df_rt, tf_rt, d_batch, df_batch, tf_batch));
			
			collector.emit(new Values(tf_rt + tf_batch, d_rt + d_batch, df_rt + df_batch));
		}
		catch (Exception e) {
			log.error("Exception", e);
		}
	}

}
