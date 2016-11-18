package org.learningstorm.tfidf.functions;

import java.io.BufferedReader;
import java.io.FileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DocumentFetchFunction extends BaseFunction {
	private static final long serialVersionUID = 7825583371660313147L;
	private Logger log = LoggerFactory.getLogger(DocumentFetchFunction.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String url = tuple.getStringByField("url");
		String document = readDocument(url);
		
		collector.emit(new Values(
				document, url.trim(), "file"));
	}

	private String readDocument(String url) {
		BufferedReader documentReader = null;
		
		try {
			documentReader = new BufferedReader(
					new FileReader(url));
			
			String line = documentReader.readLine();
			StringBuilder documentBuilder = new StringBuilder();
			
			while ( line != null ) {
				documentBuilder.append(line);
				line = documentReader.readLine();
				
				if ( line != null ) {
					documentBuilder.append('\n');
				}
			}
			
			return documentBuilder.toString();
		}
		catch (Exception e) {
			log.error("EXCEPTION", e);
		}
		finally {
			if ( documentReader != null )
				try {
						documentReader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		
		return null;
	}
}
