package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;

import edu.wiki.modify.IndexModifier;
import edu.wiki.search.ESASearcher;

public class TestWordsim353 {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		ESASearcher searcher = new ESASearcher();
		String line;
		double val;
		
		// read Wordsim-353 human judgements
		InputStream is = IndexModifier.class.getResourceAsStream("/config/wordsim353-combined.tab");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		br.readLine(); //skip first line
		System.out.println("Word 1\tWord 2\tHuman (mean)\tScore");
		while((line = br.readLine()) != null){
			final String [] parts = line.split("\t");
			if(parts.length != 3)
				break;
			
			val = searcher.getRelatedness(parts[0], parts[1]);
			
			System.out.println(line + "\t" + val);
			
		}
		br.close();
		
	}

}
