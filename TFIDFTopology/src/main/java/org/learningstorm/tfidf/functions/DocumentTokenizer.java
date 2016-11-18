package org.learningstorm.tfidf.functions;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.learningstorm.tfidf.common.TfidfTopologyFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DocumentTokenizer extends BaseFunction {
	private static final long serialVersionUID = 7550502542422476943L;
	Logger log = LoggerFactory.getLogger(DocumentTokenizer.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String documentContents = tuple.getStringByField(TfidfTopologyFields.DOCUMENT);
		TokenStream ts = null;
		
		try {
			ts = new StopFilter(Version.LUCENE_30,
					new StandardTokenizer(Version.LUCENE_30,
							new StringReader(documentContents)),
					StopAnalyzer.ENGLISH_STOP_WORDS_SET);
			
			CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
			while ( ts.incrementToken() ) {
				String lemma = MorphaStemmer.stemToken(termAtt.toString());
				lemma = lemma.trim().replaceAll("\n", "").replaceAll("\r", "");
				collector.emit(new Values(lemma));
			}
		}
		catch (IOException e) {
			log.error("EXCEPTION", e);
		}
		finally {
			if ( ts != null ) {
				try {
					ts.close();
				} catch (IOException e) {
					log.error(e.toString());
				}
			}
		}
	}
}
