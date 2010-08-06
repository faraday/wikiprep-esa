package edu.wiki.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.wiki.index.ESASimilarity;
import edu.wiki.index.WikipediaAnalyzer;

/**
 * Performs search on Lucene index.
 * 
 * Usage: WikipediaNormalSearcher <Lucene index location>
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 * 
 */
public class WikipediaNormalSearcher {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
	    File indexDir = new File(args[0]);

		
	    BufferedReader br = new BufferedReader(
	            new InputStreamReader(System.in));
	    String queryString = br.readLine();
	    Directory fsDir = FSDirectory.open(indexDir);

	    IndexSearcher searcher = new IndexSearcher(fsDir);
	    searcher.setSimilarity(new ESASimilarity());
	    QueryParser parser = new QueryParser(Version.LUCENE_CURRENT, "contents",new WikipediaAnalyzer());

	    long sTime,eTime;

	        System.out.println("searching for: " + queryString);
	        Query gabQuery = parser.parse(queryString);
	        System.out.println(gabQuery);
	        sTime = System.currentTimeMillis();
	        TopDocs results = searcher.search(gabQuery,20);
	        eTime = System.currentTimeMillis();
	        System.out.println("total hits: " + results.totalHits);
	        ScoreDoc[] hits = results.scoreDocs;
	        for (ScoreDoc hit : hits) {
	            Document doc = searcher.doc(hit.doc);
	            System.out.printf("%5.3f %s\n",
	                              hit.score, doc.get("title"));
	            //System.out.println(searcher.explain(gabQuery, hit.doc));
	        }

	    searcher.close();
	    
	    System.out.println("Time (ms): "+(eTime-sTime));

	}

}
