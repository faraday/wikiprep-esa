package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;

public class TestESAVectors {
	
	static Connection connection;
	static Statement stmtQuery;
	
	static String strTitles = "SELECT id,title FROM article WHERE id IN ";
	
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
		
		stmtQuery = connection.createStatement();
		stmtQuery.setFetchSize(100);

  }

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		ESASearcher searcher = new ESASearcher();
		initDB();
		
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
		String text = in.readLine();
		
		IConceptVector cvBase = searcher.getConceptVector(text);
		IConceptVector cv = searcher.getNormalVector(cvBase,10);
		
		if(cv == null){
			System.exit(1);
		}
		
		IConceptIterator it = cv.orderedIterator();
		
		HashMap<Integer, Double> vals = new HashMap<Integer, Double>(10);
		HashMap<Integer, String> titles = new HashMap<Integer, String>(10);
		
		String inPart = "(";
		
		int count = 0;
		while(it.next() && count < 10){
			inPart += it.getId() + ",";
			vals.put(it.getId(),it.getValue());
			count++;
		}
		
		inPart = inPart.substring(0,inPart.length()-1) + ")";
				
		ResultSet r = stmtQuery.executeQuery(strTitles + inPart);
		while(r.next()){
			titles.put(r.getInt(1), new String(r.getBytes(2),"UTF-8")); 
		}
		
		count = 0;
		it.reset();
		while(it.next() && count < 10){
			int id = it.getId();
			System.out.println(id + "\t\t" + titles.get(id) + "\t\t" + vals.get(id));
			count++;
		}
		

	}

}
