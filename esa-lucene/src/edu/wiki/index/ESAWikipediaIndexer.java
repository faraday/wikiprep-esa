package edu.wiki.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Performs indexing with Lucene.
 * Keeps term frequency vectors for further use.
 * 
 * Usage: ESAWikipediaIndexer <Lucene index location>
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 *
 */
public class ESAWikipediaIndexer {
	
	  private IndexWriter writer;
	  
	  static Connection connection = null;
	  static Statement stmtArticle;
	  static PreparedStatement pstmt;
	  static Statement stmtLimit;
	  static String strArticleQuery = "SELECT a.id,a.title,t.old_text FROM article a, text t WHERE ? <= a.id AND a.id < ? AND a.id = t.old_id";
	  
	  static String strLimitQuery = "SELECT MAX(id) FROM article;";
	  static String strPrQuery = "SELECT MAX(score) FROM pagerank;";
	  
	  static int limitID; 
	  int addCount = 0;
	  
	  public static void initDB() throws ClassNotFoundException, SQLException, IOException {
			// Load the JDBC driver 
			String driverName = "com.mysql.jdbc.Driver"; // MySQL Connector 
			Class.forName(driverName); 
			
			// read DB config
			InputStream is = ESAWikipediaIndexer.class.getResourceAsStream("/config/db.conf");
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String serverName = br.readLine();
			String mydatabase = br.readLine();
			String username = br.readLine(); 
			String password = br.readLine();
			br.close();

			// Create a connection to the database 
			String url = "jdbc:mysql://" + serverName + "/" + mydatabase; // a JDBC url 
			connection = DriverManager.getConnection(url, username, password);
			
			pstmt = connection.prepareStatement(strArticleQuery);
			pstmt.setFetchSize(200);
			
			stmtLimit = connection.createStatement();
			ResultSet res = stmtLimit.executeQuery(strLimitQuery);
			res.next();
			limitID = res.getInt(1);
			
			stmtLimit.close();
		}
	  
	  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		
	    if(args.length < 1){
	    	System.out.println("Usage: ESAWikipediaIndexer <index path>");
	    	System.exit(-1);
	    }

	    String s = args[0];
	    
	    initDB();

	    ESAWikipediaIndexer indexer = null;
	    try {
	    	Directory fsdir = FSDirectory.open(new File(s));
	      indexer = new ESAWikipediaIndexer(fsdir);
	    } catch (Exception ex) {
	      System.out.println("Cannot create index..." + ex.getMessage());
	      System.exit(-1);
	    }

	    indexer.indexDB();
	    

	    //===================================================
	    //after adding, we always have to call the
	    //closeIndex, otherwise the index is not created    
	    //===================================================
	    indexer.closeIndex();
	  }

	  /**
	   * Constructor
	   * @param indexDir the name of the folder in which the index should be created
	   * @throws java.io.IOException
	   */
	  ESAWikipediaIndexer(Directory indexDir) throws IOException {
	    // the boolean true parameter means to create a new index everytime, 
	    // potentially overwriting any existing files there.
	    writer = new IndexWriter(indexDir, new WikipediaAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED); 
	  }

	  /**
	   * Indexes a file or directory
	   * @param fileName the name of a text file or a folder we wish to add to the index
	   * @throws java.io.IOException
	 * @throws SQLException 
	   */
	  public void indexDB() throws IOException, SQLException {
	    
	    int originalNumDocs = writer.numDocs();
	    int id = 0;
	    String title;
	    Blob text_blob;
	    // float prScore;
	    	    
	    writer.setSimilarity(new ESASimilarity());
	    
	    for(int kid = 0;kid<limitID;){
	    	id = 0;
	    	
	    	pstmt.setInt(1, kid);
	    	pstmt.setInt(2, kid+400);
	    	ResultSet res = pstmt.executeQuery();
	    	
	    	
	    	while(res.next()){	// there are articles to process 
				id = res.getInt(1);
				title = new String(res.getBytes(2));
				text_blob = res.getBlob(3);
				//prScore = res.getFloat(4);
				
				try {
			        Document doc = new Document();

			        //===================================================
			        // add contents of file
			        //===================================================
			        
			        // doc.add(new Field("contents", new InputStreamReader(text_blob.getBinaryStream())));

			        doc.add(new Field("contents", new InputStreamReader(text_blob.getBinaryStream()),Field.TermVector.WITH_OFFSETS));
			        // doc.add(new Field("contents", new StringReader(wtext), Field.TermVector.WITH_OFFSETS));
			        
			        // doc.add(new Field("contents", wtext, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
			        
			        // ===
			        // second field - id
			        // ===  
			        doc.add(new Field("id", String.valueOf(id),
			        		Field.Store.YES,
			        		Field.Index.NOT_ANALYZED));
			        
			        // ====
			        // third field - title
			        // ====
			        doc.add(new Field("title", title,
			                Field.Store.YES,
			                Field.Index.NOT_ANALYZED));

			        writer.addDocument(doc);
			        //System.out.println("Added: " + id);
			        addCount++;
			        
			      } catch (Exception e) {
			        System.out.println("Could not add: " + id);
			      }
				
			}
	    	
	    	if(id > 0){
	    		kid = id + 1;
	    	}
	    	else {
	    		kid = kid + 400;
	    	}
	    	
	    	System.out.println("Added: " + addCount);
	    	
	    }
	    
	    int newNumDocs = writer.numDocs();
	    System.out.println("");
	    System.out.println("************************");
	    System.out.println((newNumDocs - originalNumDocs) + " documents added.");
	    System.out.println("************************");

	  }

	  /**
	   * Close the index.
	   * @throws java.io.IOException
	   */
	  public void closeIndex() throws IOException {
	    writer.optimize();
	    writer.close();
	  }
}
