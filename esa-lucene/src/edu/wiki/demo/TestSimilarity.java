package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

import edu.wiki.search.ESASearcher;

public class TestSimilarity {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		ESASearcher searcher = new ESASearcher();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
		String doc1 = br.readLine();
		String doc2 = br.readLine();
		br.close();
		
		System.out.println(searcher.getRelatedness(doc1, doc2));
	}

}
