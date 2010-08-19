package edu.wiki.modify;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Performs pruning on the index with a sliding window,
 * as explained in Gabrilovich et al. (2009)
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class IndexPruner {
		
	static Connection connection = null;
	static Statement stmtLink;
	static Statement pstmtTerm;
	
	static String strLoadData = "LOAD DATA LOCAL INFILE 'mod.txt' INTO TABLE tfidf FIELDS ENCLOSED BY \"'\"";
	
	static String strTermQuery = "SELECT t.term, t.doc, t.tfidf FROM otfidf t WHERE t.term IN (%s) ORDER BY t.term, t.tfidf DESC";
	
	static String strLimitTerms = "SELECT COUNT(*) FROM terms";
	static String strAllTerms = "SELECT * FROM terms";
	
	static int numTerms;
	
	static int WINDOW_SIZE = 100;
	static float WINDOW_THRES= 0.05f;
	
	static int PARALEL_TERM = 3;
	
	public static void initDB() throws ClassNotFoundException, SQLException, IOException {
		// Load the JDBC driver 
		String driverName = "com.mysql.jdbc.Driver"; // MySQL Connector 
		Class.forName(driverName); 
		
		// read DB config
		InputStream is = IndexPruner.class.getResourceAsStream("/config/db.conf");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String serverName = br.readLine();
		String mydatabase = br.readLine();
		String username = br.readLine(); 
		String password = br.readLine();
		br.close();

		// Create a connection to the database 
		String url = "jdbc:mysql://" + serverName + "/" + mydatabase; // a JDBC url 
		connection = DriverManager.getConnection(url, username, password);
		
		pstmtTerm = connection.createStatement();
		pstmtTerm.setFetchSize(2000);
		
		stmtLink = connection.createStatement();
		stmtLink.setFetchSize(200);
		
		stmtLink.execute("DROP TABLE IF EXISTS otfidf");
		stmtLink.execute("RENAME TABLE tfidf TO otfidf");
		
		stmtLink.execute("DROP TABLE IF EXISTS tfidf");
		stmtLink.execute("CREATE TABLE tfidf (" +
				"term VARBINARY(255), doc INT," +
				"tfidf FLOAT " +
				") DEFAULT CHARSET=binary");
		
		ResultSet rs = stmtLink.executeQuery(strLimitTerms);
		rs.next();
		numTerms = rs.getInt(1);
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
	    
	    initDB();
	    
	    long sTime, eTime;
	    
	    sTime = System.currentTimeMillis();
	    
	    String term;
	    String[] terms = new String[numTerms];
	    int doc;
	    float tfidf;
	    
	    int mark, windowMark;
	    float first = 0, last = 0, highest = 0;
	    int qcount = 0;
	    float [] window = new float[WINDOW_SIZE];
	    	    
		// FileWriter bw = new FileWriter("mod.txt");
	    FileOutputStream fos = new FileOutputStream("mod.txt");
		OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
	    
	    // read all terms
	    ResultSet res = stmtLink.executeQuery(strAllTerms);
	    int ti = 0;
	    while(res.next()){
	    	terms[ti++] = new String(res.getBytes(1),"UTF-8");
	    }
	    
	    // process term vectors
	    ResultSet resTerm;
	    String tquery, prevTerm = null;
	    boolean skip = false;
	    
	    
	    for(int i=0;i<numTerms;){
	    	
		    String inClause = "";
	    	
	    	for(int k=0;i<numTerms && k < PARALEL_TERM;k++,i++){
	    		inClause += '"' + terms[i] + "\",";
	    	}
	    	inClause = inClause.substring(0, inClause.length()-1);
	    	
	    	tquery = strTermQuery.replace("%s", inClause);

	    	resTerm = pstmtTerm.executeQuery(tquery);

	    	mark = 0;
	    	windowMark = 0;

	    	while(resTerm.next()){
	    		term = new String(resTerm.getBytes(1),"UTF-8");
	    		doc = resTerm.getInt(2);
	    		tfidf = resTerm.getFloat(3);
	    		
	    		// next term
	    		if(prevTerm == null || !prevTerm.equals(term)){
	    			mark = 0;
	    			windowMark = 0;
	    			prevTerm = term;
	    			skip = false;
	    			highest = first = last = 0;
	    		}
	    		else if(skip){
	    			// truncating...
	    			mark++;
	    			continue;
	    		}
	    		
	    		window[windowMark] = tfidf;
	    		
	    		if(mark == 0){
	    			highest = tfidf;
	    			first = tfidf;
	    		}
	    		    		
	    		if(mark < WINDOW_SIZE){
	    			osw.write("'" +  term.replace("\\","\\\\").replace("'","\\'") + "'\t"+doc+"\t"+tfidf+"\n");
	    			qcount++;	    			
	    		}
	    		else if( highest*WINDOW_THRES < (first - last) ){
	    			osw.write("'" +  term.replace("\\","\\\\").replace("'","\\'") + "'\t"+doc+"\t"+tfidf+"\n");
	    			if(windowMark < WINDOW_SIZE-1){
	    				first = window[windowMark+1];
	    			}
	    			else {
	    				first = window[0];
	    			}
	    		}
	    		
	    		else {
	    			// truncate
	    			// System.out.println("Truncated: " + term + " - first: " + first + " last: " + last + " mark: " + mark);
	    			skip = true;
	    			mark++;
	    			continue;
	    		}
	    		
	    		// for DEBUGGING
//	    		if(mark >= WINDOW_SIZE){
//	    			System.out.println(term + " mark: " + mark + " " + highest + " " + highest*WINDOW_THRES + " vs. " + (first - last) + " f: " + first + " l: " + last);
//	    		}
	    		
	    		last = tfidf;

	    		mark++;
	    		windowMark++;
	    		
	    		windowMark = windowMark % WINDOW_SIZE;
	    	}

	    	// write to DB
	    	if(qcount > 100000){
	    		osw.flush();
	    		stmtLink.execute(strLoadData);
	    		qcount = 0;
	    		// bw = new FileWriter("mod.txt",false);
	    		fos = new FileOutputStream("mod.txt",false);
				osw = new OutputStreamWriter(fos,"UTF-8");
	    	}

	    }
	    
	    // write last part to DB
    	if(qcount > 0){
    		osw.flush();
    		stmtLink.execute(strLoadData);
    		qcount = 0;
    		// bw = new FileWriter("mod.txt",false);
    		fos = new FileOutputStream("mod.txt",false);
			osw = new OutputStreamWriter(fos,"UTF-8");
    	}

	   
		stmtLink.execute("CREATE INDEX idx_term ON tfidf (term(32))");
		stmtLink.execute("DROP TABLE otfidf");
	    
	    eTime = System.currentTimeMillis();
	    
		System.out.println("Total TIME (sec): "+ (eTime-sTime)/1000.0);
	    
	    connection.close();
	    
	    osw.close();
	    
	}

}
