package edu.wiki.concept;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.search.IScorer;


public class ConceptVectorSimilarity {

	IScorer m_scorer;
	
	public ConceptVectorSimilarity( IScorer scorer ) {
		m_scorer = scorer;
	}
	
	public double calcSimilarity( IConceptVector v0, IConceptVector v1 ) {
		m_scorer.reset( v0.getData(), v1.getData(), 1 );
		
		IConceptIterator it0 = v0.iterator();
		while( it0.next() ) {
			double value1 = v1.get( it0.getId() );
			if( value1 > 0 ) {
				m_scorer.addConcept( it0.getId(), it0.getValue(), it0.getId(), value1, 1 );
			}
		}
		
		m_scorer.finalizeScore( v0.getData(), v1.getData() );
		
		return m_scorer.getScore();
	}
	
}
