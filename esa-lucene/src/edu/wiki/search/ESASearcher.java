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
import java.sql.Statement;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.queryParser.ParseException;

import edu.wiki.index.WikipediaAnalyzer;

/**
 * Performs search on the index located in database.
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class ESASearcher {
	static Connection connection;
	static Statement stmtQuery;
	
	static String strQuery = "SELECT k.* FROM (SELECT t.doc, SUM(t.tfidf)/%d AS tfidf, a.title FROM tfidf t, article a WHERE t.term IN %s AND t.doc = a.id GROUP BY t.doc) AS k ORDER BY k.tfidf DESC LIMIT 0,30";

	
	public static void initDB() throws ClassNotFoundException, SQLException, IOException {
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
		
		// stmtQuery = connection.prepareStatement(strQuery);
		stmtQuery = connection.createStatement();
		stmtQuery.setFetchSize(200);
  }
	
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, SQLException {
		initDB();
		
		BufferedReader br = new BufferedReader(
	            new InputStreamReader(System.in));
	    String q = br.readLine();
        
        WikipediaAnalyzer analyzer = new WikipediaAnalyzer(); // or any other analyzer 
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(q));
         
        String strTerm = null;
        
        int doc;
        float score;
        String title;
                
        int numTerms = 0;
        
        String qterm = "(";
         
        try {
            ts.reset();
            while (ts.incrementToken()) { 
            	
              TermAttribute t = ts.getAttribute(TermAttribute.class);
              strTerm = t.term();
              
              qterm += "\"" + strTerm.replace("\\","\\\\").replace("\"","\\\"") + "\",";
              
              numTerms++;	

            } 
            ts.end();
            ts.close();
            
            qterm = qterm.substring(0, qterm.length()-1) + ")";
            System.out.println(qterm);
            
            System.out.println(strQuery.replace("%s", qterm).replace("%d",String.valueOf(numTerms)));
            stmtQuery.execute(strQuery.replace("%s", qterm).replace("%d",String.valueOf(numTerms)));
            
            int count = 0;
            
          ResultSet rs = stmtQuery.getResultSet();
          
          while(rs.next()){
        	  doc = rs.getInt(1);
        	  score = rs.getFloat(2);
        	  title = new String(rs.getBytes(3),"UTF-8");
        	  
        	  System.out.println(title + " " + score);
        	  
        	  
        	  count++;
        	  if(count > 20)
        		  break;

          }
          
            
        }
        catch (IOException e1) {
            e1.printStackTrace();
        }

		
		
	    connection.close();
	}
}
