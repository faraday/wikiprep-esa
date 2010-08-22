package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

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
		String tkey;
		float score;
		int rank;
		float sum, val;
		int numPairs;
		float spearman;
		
		HashMap<String, Float> human = new HashMap<String, Float>();
		HashMap<String, Float> esa = new HashMap<String, Float>();
		
		HashMap<String, Integer> humanRank = new HashMap<String, Integer>();
		HashMap<String, Integer> esaRank = new HashMap<String, Integer>();
		
		// read Wordsim-353 human judgements
		numPairs = 0;
		InputStream is = IndexModifier.class.getResourceAsStream("/config/wordsim353-combined.tab");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		br.readLine(); //skip first line
		while((line = br.readLine()) != null){
			final String [] parts = line.split("\t");
			if(parts.length != 3)
				break;
			
			// part1,part2 = terms, part3 = score
			tkey = parts[0] + "||" + parts[1];
			score = Float.valueOf(parts[2]);
			
			human.put(tkey, score);
			numPairs++;
		}
		br.close();
		
		for(String t : human.keySet()){
			final String [] terms = t.split("\\|\\|");
			score = (float) searcher.getRelatedness(terms[0], terms[1]);
			esa.put(t, score);
		}
		

		//Sort human term-term keys (descending).
		ArrayList<String> hkeys = new ArrayList(human.keySet());
		
		final Map langForComp = human;
		Collections.sort(hkeys, 
			new Comparator(){
				public int compare(Object left, Object right){
					String leftKey = (String)left;
					String rightKey = (String)right;
					
					Float leftValue = (Float)langForComp.get(leftKey);
					Float rightValue = (Float)langForComp.get(rightKey);
					return leftValue.compareTo(rightValue);
				}
			});
		Collections.reverse(hkeys);
		
		
		//Sort ESA term-term keys (descending).
		ArrayList<String> esakeys = new ArrayList(esa.keySet());
		
		final Map langForComp2 = esa;
		Collections.sort(esakeys, 
			new Comparator(){
				public int compare(Object left, Object right){
					String leftKey = (String)left;
					String rightKey = (String)right;
					
					Float leftValue = (Float)langForComp2.get(leftKey);
					Float rightValue = (Float)langForComp2.get(rightKey);
					return leftValue.compareTo(rightValue);
				}
			});
		Collections.reverse(esakeys);
		
		rank = 1;	// init
		for(String t : hkeys){
			humanRank.put(t, rank++);
		}
		
		rank = 1;	// init
		for(String t : esakeys){
			esaRank.put(t, rank++);
		}
		
		// compute squared rank difference sum
		sum = 0;
		System.out.println("Term1\tTerm2\tHuman rank\tESA rank\tHuman score\tESA score");
		for(String t : hkeys){
			System.out.println(t.replace("||", "\t") + "\t" + humanRank.get(t) + 
					"\t" + esaRank.get(t) + "\t" + human.get(t) + 
					"\t" + esa.get(t));
			val = humanRank.get(t) - esaRank.get(t);
			sum += val * val;
		}
		
		spearman = 1 - (6 * sum / (numPairs * (numPairs*numPairs - 1)));
		
		System.out.println("\nSpearman correlation: " + spearman);

	}

}
