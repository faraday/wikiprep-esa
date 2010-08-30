package edu.wiki.search;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.wiki.index.WikipediaAnalyzer;

public class NormalizedWikipediaDistance {

	private IndexSearcher searcher;
	private QueryParser qparser;
    private Query wQuery;
    private TopDocs wResults;
    
    int numWikiDocs;
    
    public class NumRes {
    	public int res1;
        public int res2;
        public int resCommon;
        
        public NumRes() {
        	res1 = res2 = resCommon = 0;
        }
        
        public void reset(){
        	res1 = res2 = resCommon = 0;
        }
    }
    
    NumRes nres = new NumRes();
    
    public NormalizedWikipediaDistance(String indexPath){
    	Directory fsDir = null;
		try {
			fsDir = FSDirectory.open(new File(indexPath));
			searcher = new IndexSearcher(fsDir);
			numWikiDocs = searcher.maxDoc();
			qparser = new QueryParser(Version.LUCENE_CURRENT, "contents", new WikipediaAnalyzer());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    }
	
	private int freqSearch(String phrase) throws ParseException, IOException{
    	wQuery = qparser.parse("\""+QueryParser.escape(phrase)+"\"");
    	// wQuery = qparser.parse(QueryParser.escape(phrase));
        wResults = searcher.search(wQuery,1);
        return wResults.totalHits;
    }
	
	/**
     * Search to find the probability of occurrence for two phrases
     * @param queryString
     * @param exactPhrase
     * @return
     * @throws ParseException
     * @throws IOException
     */
    private int occurSearch(String phrase1, String phrase2) throws ParseException, IOException{
    	wQuery = qparser.parse("\""+QueryParser.escape(phrase1)+"\" AND " + "\""+QueryParser.escape(phrase2)+"\"");
    	// wQuery = qparser.parse("(" + QueryParser.escape(phrase1)+") AND (" + QueryParser.escape(phrase2) + ")");
        wResults = searcher.search(wQuery,1);
        return wResults.totalHits;
    }
	
	public double getDistance(String label1, String label2){
    	float f1 = 0.0f, f2 = 0.0f;
    	float fCommon = 0.0f;
    	
    	nres.reset();
    	
    	try {
			nres.res1 = freqSearch(label1);
			f1 = nres.res1;
			nres.res2 = freqSearch(label2);
			f2 = nres.res2;
			nres.resCommon = occurSearch(label1, label2);
			fCommon = nres.resCommon;
			
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(f1 == 0 || f2 == 0){
			return -1f;	// undefined
		}
		
		// if((fCommon == 0) && (f1 > 0 || f2 > 0) ){
		if(fCommon == 0){
			return 10000.0f;	// infinite distance
		}
		
		f1 *= 2;	f2 *= 2;	fCommon *= 2;	// just generalize
		
		double log1, log2 , logCommon, maxlog, minlog;
		log1 = Math.log(f1);	log2 = Math.log(f2);	logCommon = Math.log(fCommon);
		maxlog = Math.max(log1, log2);	minlog = Math.min(log1, log2);
		
		return (maxlog - logCommon) / (Math.log(numWikiDocs) - minlog);   
    	
    }
	
	public NumRes getMatches(){
		return nres;
	}
}
