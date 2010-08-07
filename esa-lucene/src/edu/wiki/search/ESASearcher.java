package edu.wiki.search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.sql.PreparedStatement;
import java.util.Arrays;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.util.HeapSort;

/**
 * Performs search on the index located in database.
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class ESASearcher {
	Connection connection;
	
	PreparedStatement pstmtQuery;
	
	WikipediaAnalyzer analyzer;
	
	String strTermQuery = "SELECT t.doc,t.tfidf FROM tfidf t WHERE t.term = ?";
	
	String strMaxConcept = "SELECT MAX(id) FROM article";

	int maxConceptId;
	
	int[] ids;
	double[] values;
	
	public void initDB() throws ClassNotFoundException, SQLException, IOException {
		// Load the JDBC driver 
		String driverName = "com.mysql.jdbc.Driver"; // MySQL Connector 
		Class.forName(driverName); 
		
		// read DB config
		InputStream is = ESASearcher.class.getResourceAsStream("/config/db.conf");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String serverName = br.readLine();
		String mydatabase = br.readLine();
		String username = br.readLine(); 
		String password = br.readLine();
		br.close();

		// Create a connection to the database 
		String url = "jdbc:mysql://" + serverName + "/" + mydatabase; // a JDBC url 
		connection = DriverManager.getConnection(url, username, password);
		
		pstmtQuery = connection.prepareStatement(strTermQuery);
		pstmtQuery.setFetchSize(500);
		
		ResultSet res = connection.createStatement().executeQuery(strMaxConcept);
		res.next();
		maxConceptId = res.getInt(1) + 1;
  }
	
	public ESASearcher() throws ClassNotFoundException, SQLException, IOException{
		initDB();
		analyzer = new WikipediaAnalyzer();
		
		ids = new int[maxConceptId];
		values = new double[maxConceptId];
		
	}
	
	@Override
	protected void finalize() throws Throwable {
        connection.close();
		super.finalize();
	}
	
	public IConceptVector extractVector(String query) throws IOException, SQLException{
		String strTerm;
		int numTerms = 0;
		ResultSet rs;
		int doc;
		float score;
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));

		Arrays.fill( values, 0 );

		for( int i=0; i<ids.length; i++ ) {
			ids[i] = i;
		}
        
        ts.reset();
        
        while (ts.incrementToken()) { 
        	
            TermAttribute t = ts.getAttribute(TermAttribute.class);
            strTerm = t.term();
                        
            pstmtQuery.setString(1, strTerm);
            pstmtQuery.execute();
            
            rs = pstmtQuery.getResultSet();
            
            while(rs.next()){
          	  doc = rs.getInt(1);
          	  score = rs.getFloat(2);
          	  
          	  values[doc] += score;
            }
            
            numTerms++;	

        } 
        
        ts.end();
        ts.close();      
        
        if(numTerms == 0){
        	return null;
        }
        
        HeapSort.heapSort( values, ids );
        
        IConceptVector newCv = new TroveConceptVector(ids.length);
		for( int i=ids.length-1; i>=0 && values[i] > 0; i-- ) {
			newCv.set( ids[i], values[i] / numTerms );
		}
		
		return newCv;
	}
	

}
