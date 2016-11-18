package org.learningstorm.tfidf.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TfidfExpression extends BaseFunction {
	private static final long serialVersionUID = 5994635677464261214L;
	private Logger log = LoggerFactory.getLogger(TfidfExpression.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d = (double)tuple.getLongByField("d");
			double df = (double)tuple.getLongByField("df");
			double tf = (double)tuple.getLongByField("tf");
			
			log.debug(String.format("d=%f;df=%f;tf=%f", d, df, tf));
			
			double tfidf = tf * Math.log(d / (1 + df));
			log.debug(String.format(
					"Emitting new TFIDF(term, Document): (%s, %s) = %f",
					tuple.getStringByField("term"),
					tuple.getStringByField("documentId"),
					tfidf));
			
			collector.emit(new Values(tfidf));
		}
		catch ( Exception e ) {
			log.error("EXCEPTION", e);
		}
	}
}
