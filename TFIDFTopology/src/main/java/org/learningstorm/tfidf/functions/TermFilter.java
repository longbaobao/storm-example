package org.learningstorm.tfidf.functions;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TermFilter extends BaseFunction {
	private static final long serialVersionUID = 3126803911679990126L;
	private SpellChecker spellChecker;
	private List<String> filterTerms = Arrays.asList(new String[]{"http"});
	private Logger log = LoggerFactory.getLogger(TermFilter.class);
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		File dir = new File(System.getProperty("user.home") + "/dictionaries");
		Directory directory;
		
		try {
			directory = FSDirectory.open(dir);
			spellChecker = new SpellChecker(directory);
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
			URL dictionaryFile = TermFilter.class.getResource("/dictionaries/fulldictionary00.txt");
			spellChecker.indexDictionary(
					new PlainTextDictionary(new File(dictionaryFile.toURI())), 
					config, true);
		} catch (Exception e) {
			log.error("EXCEPTION", e);
		}
	}
	
	private boolean shouldKeep(String stem) {
		if ( stem == null ) {
			return false;
		}
		if ( stem.equals("") ) {
			return false;
		}
		if ( filterTerms.contains(stem) ) {
			return false;
		}
		
		try {
			Integer.parseInt(stem);
			return false;
		}
		catch ( Exception e ) {
			log.error("EXCEPTION", e);
		}
		
		try {
			Double.parseDouble(stem);
			return false;
		}
		catch ( Exception e ) {
			log.error("EXCEPTION", e);
		}
		
		try {
			return spellChecker.exist(stem);
		}
		catch ( Exception e ) {
			log.error("EXCEPTION", e);
			return false;
		}
	}
	
	public boolean isKeep(TridentTuple tuple) {
		log.debug("Filtering Tuple");
		
		return shouldKeep(tuple.getString(0));
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if ( isKeep(tuple) ) {
			collector.emit(tuple);
		}
	}
}
