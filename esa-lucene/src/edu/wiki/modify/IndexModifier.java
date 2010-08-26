package edu.wiki.modify;

import edu.wiki.util.HeapSort;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntFloatHashMap;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
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
	static PreparedStatement pstmtVector;
			
	// static String strLoadData = "LOAD DATA LOCAL INFILE 'vector.txt' INTO TABLE idx FIELDS ENCLOSED BY \"'\"";
	static String strVectorQuery = "INSERT INTO idx (term,vector) VALUES (?,?)";
	
	static String strTermLoadData = "LOAD DATA LOCAL INFILE 'term.txt' INTO TABLE terms FIELDS ENCLOSED BY \"'\"";
	
	static String strAllInlinks = "SELECT target_id,inlink FROM inlinks";
	
	static String strLimitQuery = "SELECT COUNT(id) FROM article;";
		
	private static IndexReader reader = null;
	
	static int limitID;
	
	private static TIntDoubleHashMap inlinkMap;
		
	static int WINDOW_SIZE = 100;
	static float WINDOW_THRES= 0.05f;
	
	static DecimalFormat df = new DecimalFormat("#.########");
		
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
		String url = "jdbc:mysql://" + serverName + "/" + mydatabase + "?useUnicode=yes&characterEncoding=UTF-8"; // a JDBC url 
		connection = DriverManager.getConnection(url, username, password);
		
		stmtLink = connection.createStatement();
		stmtLink.setFetchSize(200);
		
		stmtLink.execute("DROP TABLE IF EXISTS tfidf");
		stmtLink.execute("CREATE TABLE tfidf (" +
				"term VARBINARY(255), doc INT," +
				"tfidf FLOAT " +
				") DEFAULT CHARSET=binary");
		
		stmtLink.execute("DROP TABLE IF EXISTS idx");
		stmtLink.execute("CREATE TABLE idx (" +
				"term VARBINARY(255)," +
				"vector MEDIUMBLOB " +
				") DEFAULT CHARSET=binary");
		
    	stmtLink.execute("DROP TABLE IF EXISTS terms");
    	stmtLink.execute("CREATE TABLE terms (" +
				"term VARBINARY(255)," +
				"idf FLOAT " +
				") DEFAULT CHARSET=binary");

		
		stmtLink = connection.createStatement();
		ResultSet res = stmtLink.executeQuery(strLimitQuery);
		res.next();
		limitID = res.getInt(1);
		
		
		// read inlink counts	
		inlinkMap = new TIntDoubleHashMap(limitID);
		
		int targetID, numInlinks; 
		res = stmtLink.executeQuery(strAllInlinks);
		while(res.next()){
			targetID = res.getInt(1);
			numInlinks = res.getInt(2);
			inlinkMap.put(targetID, Math.log(1+Math.log(1+numInlinks)));
		}
		
		pstmtVector = connection.prepareStatement(strVectorQuery);
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws NoSuchAlgorithmException 
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
	    String term = "";
	    
	    Term t;
	    
	    int tfreq = 0;
	    float idf;
	    float tf;
	    float tfidf;
	    double inlinkBoost;
	    double sum;
	    
	    int wikiID;
	    
	    int hashInt;
	    	    
	    int numDocs = reader.numDocs();
	    
	    TermEnum tnum = reader.terms();
	    HashMap<String, Float> idfMap = new HashMap<String, Float>(500000);
	    
	    HashMap<String, Float> tfidfMap = new HashMap<String, Float>(5000);

	    HashMap<String, Integer> termHash = new HashMap<String, Integer>(500000);
	    	    	    
	    FileOutputStream fos = new FileOutputStream("vector.txt");
		OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
	    
	    tnum = reader.terms();
	    
	    hashInt = 0;
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
	    	termHash.put(term, hashInt++);
	    	
	    }

	    
	    for(int i=0;i<maxid;i++){
	    	if(!reader.isDeleted(i)){
	    		//System.out.println(i);
	    		
	    		wikiID = Integer.valueOf(reader.document(i).getField("id").stringValue());
	    		inlinkBoost = inlinkMap.get(wikiID);
	    			    		
	    		tv = reader.getTermFreqVector(i, "contents");
	    		try {
	    			terms = tv.getTerms();
	    			
	    			int[] fq = tv.getTermFrequencies();
	    		
	    		
		    		sum = 0.0;	   
		    		tfidfMap.clear();
		    		
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
		    				    			
		    		}
		    		
		    		
		    		sum = Math.sqrt(sum);
		    		
		    		// for all terms of a document
		    		for(int k=0;k<terms.length;k++){
		    			term = terms[k];
		    			if(!idfMap.containsKey(term))
		    				continue;
		    				    			
		    			tfidf = (float) (tfidfMap.get(term) / sum * inlinkBoost);
		    			
		    				    				    			
		    			// System.out.println(i + ": " + term + " " + fq[k] + " " + tfidf);
		    			
		    			// ++++ record to DB (term,doc,tfidf) +++++
		    			osw.write(termHash.get(term) + "\t" + term + "\t" + wikiID + "\t" + df.format(tfidf) + "\n");
						
		    		}
	    		
	    		}
	    		catch(Exception e){
	    			e.printStackTrace();
	    			System.out.println("ERR: " + wikiID + " " + tv);
	    			continue;
	    		}
	    		
	    	}
	    }
	    osw.close();
	    fos.close();
	    	    
	    // sort tfidf entries according to terms
	    String[] cmd = {"/bin/sh", "-c", "sort -S 1200M -n -t\\\t -k1 < vector.txt > vsorted.txt"};
	    Process p1 = Runtime.getRuntime().exec(cmd);	
	    try {
			int exitV = p1.waitFor();
			if(exitV != 0){
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		// delete unsorted doc-score file
	    p1 = Runtime.getRuntime().exec("rm vector.txt");	
	    try {
			int exitV = p1.waitFor();
			if(exitV != 0){
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		FileInputStream fis = new FileInputStream("vsorted.txt");
		InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
		BufferedReader bir = new BufferedReader(isr);
		
		String line;
		String prevTerm = null;
		int doc;
		float score;
		TIntFloatHashMap hmap = new TIntFloatHashMap(100);
		
		// for pruning
		int mark, windowMark;
	    float first = 0, last = 0, highest = 0;
	    float [] window = new float[WINDOW_SIZE];
	    
		while((line = bir.readLine()) != null){
			final String [] parts = line.split("\t");
			term = parts[1];
			
			// prune and write the vector
			if(prevTerm != null && !prevTerm.equals(term)){
				int [] arrDocs = hmap.keys();
		    	float [] arrScores = hmap.getValues();
		    	
		    	HeapSort.heapSort(arrScores, arrDocs);
		    	
		    	// prune the vector
		    	
		    	mark = 0;
				windowMark = 0;
				highest = first = last = 0;
		    	
		    	ByteArrayOutputStream baos = new ByteArrayOutputStream(50000);
		    	DataOutputStream tdos = new DataOutputStream(baos);
		    	
		    	for(int j=arrDocs.length-1;j>=0;j--){
		    		score = arrScores[j];
		    		
		    		// sliding window
		    		
		    		window[windowMark] = score;
		    		
		    		if(mark == 0){
		    			highest = score;
		    			first = score;
		    		}
		    		    		
		    		if(mark < WINDOW_SIZE){
			    		tdos.writeInt(arrDocs[j]);
			    		tdos.writeFloat(score);
		    		}
		    		else if( highest*WINDOW_THRES < (first - last) ){
		    			tdos.writeInt(arrDocs[j]);
			    		tdos.writeFloat(score);

		    			if(windowMark < WINDOW_SIZE-1){
		    				first = window[windowMark+1];
		    			}
		    			else {
		    				first = window[0];
		    			}
		    		}
		    		
		    		else {
		    			// truncate
		    			break;
		    		}	
		    		
		    		last = score;

		    		mark++;
		    		windowMark++;
		    		
		    		windowMark = windowMark % WINDOW_SIZE;
		    		
		    	}
		    			    	
		    	ByteArrayOutputStream dbvector = new ByteArrayOutputStream();
		    	DataOutputStream dbdis = new DataOutputStream(dbvector);
		    	dbdis.writeInt(mark);
		    	dbdis.flush();
		    	dbvector.write(baos.toByteArray());
		    	dbvector.flush();
		    	
		    	dbdis.close();
		    			    			    		    		
	    		// write to DB
		    	pstmtVector.setString(1, prevTerm);
		    	pstmtVector.setBlob(2, new ByteArrayInputStream(dbvector.toByteArray()));
		    	
		    	pstmtVector.execute();
		    	
		    	tdos.close();
		    	baos.close();
				
				hmap.clear();
			}
			
			doc = Integer.valueOf(parts[2]);
			score = Float.valueOf(parts[3]);
			
			hmap.put(doc, score);
			
			prevTerm = term;
		}
		
    	bir.close();
		isr.close();
		fis.close();
    	
    	// record term IDFs
    	FileOutputStream tos = new FileOutputStream("term.txt");
		OutputStreamWriter tsw = new OutputStreamWriter(tos,"UTF-8");
    	
    	for(String tk : idfMap.keySet()){
			tsw.write("'" +  tk.replace("\\","\\\\").replace("'","\\'") + "'\t"+idfMap.get(tk)+"\n");
		}
    	osw.close();
		tsw.close();
		stmtLink.execute(strTermLoadData);
		stmtLink.execute("CREATE INDEX idx_term ON terms (term(32))");
    	
	    eTime = System.currentTimeMillis();
	    
		System.out.println("Total TIME (sec): "+ (eTime-sTime)/1000.0);
	    
		
	    reader.close();
	    connection.close();
	    
	}

}
