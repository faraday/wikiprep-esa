package edu.wiki.index;

import org.apache.lucene.search.DefaultSimilarity;

public class ESASimilarity extends DefaultSimilarity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public float idf(int docFreq, int numDocs) {
		return (float) Math.log(numDocs / (double) docFreq);
	}
	
	@Override
	public float tf(float freq) {
		return (float) (1.0 + Math.log(freq));
	}

}
