package edu.wiki.modify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Reads TF and IDF from the index and 
 * writes cosine-normalized TF.IDF values to database.
 * 
 * Normalization is performed as in Gabrilovich et al. (2009)
 * 
 * Usage: IndexModifier <Lucene index location>
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 *
 */
public class IndexModifier {
		
	static Connection connection = null;
	static Statement stmtLink;
		
	static String strLoadData = "LOAD DATA LOCAL INFILE 'mod.txt' INTO TABLE tfidf FIELDS ENCLOSED BY \"'\"";
	
	static String strAllInlinks = "SELECT target_id,inlink FROM inlinks";

	private static IndexReader reader = null;
		
	public static void initDB() throws ClassNotFoundException, SQLException, IOException {
		// Load the JDBC driver 
		String driverName = "com.mysql.jdbc.Driver"; // MySQL Connector 
		Class.forName(driverName); 
		
		// read DB config
		InputStream is = IndexModifier.class.getResourceAsStream("/config/db.conf");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String serverName = br.readLine();
		String mydatabase = br.readLine();
		String username = br.readLine(); 
		String password = br.readLine();
		br.close();

		// Create a connection to the database 
		String url = "jdbc:mysql://" + serverName + "/" + mydatabase; // a JDBC url 
		connection = DriverManager.getConnection(url, username, password);
		
		stmtLink = connection.createStatement();
		stmtLink.setFetchSize(200);
		
		stmtLink.execute("DROP TABLE IF EXISTS tfidf");
		stmtLink.execute("CREATE TABLE tfidf (" +
				"term VARBINARY(255), doc INT," +
				"tfidf FLOAT " +
				") DEFAULT CHARSET=binary");
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
	    		
		try {
	    	Directory fsdir = FSDirectory.open(new File(args[0]));
			reader = IndexReader.open(fsdir,true);
	    } catch (Exception ex) {
	      System.out.println("Cannot create index..." + ex.getMessage());
	      System.exit(-1);
	    }
	    
	    initDB();
	    
	    long sTime, eTime;
	    
	    sTime = System.currentTimeMillis();
	    
	    int maxid = reader.maxDoc();
	    TermFreqVector tv;
	    String[] terms;
	    String term;
	    
	    Term t;
	    
	    int tfreq = 0;
	    float idf;
	    float tf;
	    float tfidf;
	    double sum;
	    
	    int wikiID;
	    	    
	    int numDocs = reader.numDocs();   
	    
	    TermEnum tnum = reader.terms();
	    HashMap<String, Float> idfMap = new HashMap<String, Float>(500000);
	    
	    HashMap<String, Float> tfidfMap = new HashMap<String, Float>(5000);
	    
	    int qcount = 0;
	    	    
	    while(tnum.next()){
	    	t = tnum.term();
	    	term = t.text();
	    	
	    	tfreq = tnum.docFreq();	// get DF for the term
	    	
	    	// skip rare terms
	    	if(tfreq < 3){
	    		continue;
	    	}
	    	
	    	// idf = (float)(Math.log(numDocs/(double)(tfreq+1)) + 1.0);
	    	idf = (float)(Math.log(numDocs/(double)(tfreq))); 	
	    	// idf = (float)(Math.log(numDocs/(double)(tfreq)) / Math.log(2)); 	

	    	idfMap.put(term, idf);
	    	
	    }
	    
		FileWriter bw = new FileWriter("mod.txt");

	    
	    for(int i=0;i<maxid;i++){
	    	if(!reader.isDeleted(i)){
	    		System.out.println(i);
	    		
	    		wikiID = Integer.valueOf(reader.document(i).getField("id").stringValue());
	    		
	    		tv = reader.getTermFreqVector(i, "contents");
	    		try {
	    			terms = tv.getTerms();
	    			
	    			int[] fq = tv.getTermFrequencies();
	    		
	    		
	    		sum = 0.0;	    		
	    		
	    		// for all terms of a document
	    		for(int k=0;k<terms.length;k++){
	    			term = terms[k];
	    			if(!idfMap.containsKey(term))
	    				continue;
	    			
	    			tf = (float) (1.0 + Math.log(fq[k]));
	    			// tf = (float) (1.0 + Math.log(fq[k]) / Math.log(2));

	    			idf = idfMap.get(term);
	    			
	    			tfidf = (float) (tf * idf);
	    			tfidfMap.put(term, tfidf);
	    			
	    			sum += tfidf * tfidf;
	    				    			
	    			//System.out.println(i + ": " + term + " " + fq[k] + " " + idfMap.get(term));
	    		}
	    		
	    		
	    		sum = Math.sqrt(sum);
	    		
	    		// for all terms of a document
	    		for(int k=0;k<terms.length;k++){
	    			term = terms[k];
	    			if(!idfMap.containsKey(term))
	    				continue;
	    				    			
	    			tfidf = (float) (tfidfMap.get(term) / sum);
	    				    				    			
	    			// System.out.println(i + ": " + term + " " + fq[k] + " " + tfidf);
	    			
	    			// ++++ record to DB +++++
	    			bw.write("'" +  term.replace("\\","\\\\").replace("'","\\'") + "'\t"+wikiID+"\t"+tfidf+"\n");
					
					qcount++;
					
					if(qcount > 100000){
						bw.flush();

						stmtLink.execute(strLoadData);
						
						qcount = 0;
							
						bw = new FileWriter("mod.txt",false);
						
						
						
					}
					// +++++++++++++++++++++ 
					
					
	    		}
	    		
	    		}
	    		catch(Exception e){
	    			System.out.println("ERR: " + wikiID + " " + tv);
	    			continue;
	    		}
	    		
	    	}
	    }
	    
	    // write last part to DB
	    if(qcount > 0){
			bw.flush();
			stmtLink.execute(strLoadData);
			qcount = 0;
			bw = new FileWriter("mod.txt",false);
		}
	    
	   
		stmtLink.execute("CREATE INDEX idx_term ON tfidf (term(32))");
		
		stmtLink.execute("DROP TABLE IF EXISTS terms");
		stmtLink.execute("CREATE TABLE terms AS SELECT DISTINCT t.term FROM tfidf t");
	    
	    eTime = System.currentTimeMillis();
	    
		System.out.println("Total TIME (sec): "+ (eTime-sTime)/1000.0);
	    
		
	    reader.close();
	    connection.close();
	    
	    bw.close();

	}

}
