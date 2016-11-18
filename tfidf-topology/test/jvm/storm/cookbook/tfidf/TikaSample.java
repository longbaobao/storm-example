package storm.cookbook.tfidf;

import java.io.File;
import java.io.FileInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.tartarus.snowball.ext.PorterStemmer;
import org.xml.sax.ContentHandler;

public class TikaSample {
	
	static PorterStemmer stemmer = new PorterStemmer();
	 
    public static void main(String[] args) throws Exception {
        // parse out the directory that we want to crawl
        if (args.length != 1) {
            showUsageAndExit();
        }
 
        File directory = new File(args[0]);
        if (!directory.isDirectory()) {
            showUsageAndExit();
        }
 
        parseAllFilesInDirectory(directory);
    }
 
    private static void parseAllFilesInDirectory(File directory) throws Exception {
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                parseAllFilesInDirectory(file);
            } else {
                Parser parser = new AutoDetectParser();
                
                Metadata metadata = new Metadata();
                ParseContext parseContext = new ParseContext();
                
 
                ContentHandler handler = new BodyContentHandler(10*1024*1024);
                parser.parse(new FileInputStream(file), handler, metadata, parseContext);
 
                System.out.println("-------------------------------------------------------");
                System.out.println("File: " + file);
                for (String name : metadata.names()) {
                	System.out.println("metadata: " + name + " - " + metadata.get(name));
            	}
                if(metadata.get("Content-Type").contains("pdf")){
                	//printTerms(handler.toString());
                	for (String name : metadata.names()) {
                    	System.out.println("metadata: " + name + " - " + metadata.get(name));
                	}
                	//System.out.println("Content: " + handler.toString());
                }
            }
        }
    }
 
    private static void showUsageAndExit() {
        System.err.println("Usage: java TikaSample <directory to crawl>");
        System.exit(1);
    }
}
