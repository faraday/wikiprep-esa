package edu.wiki.api.concept.scorer;

import edu.wiki.api.concept.IConceptVectorData;
import edu.wiki.api.concept.search.IScorer;


public class CosineScorer implements IScorer {

	double m_sum = 0;
	
	public double getScore() {
		return m_sum;
	}

	public void reset( IConceptVectorData queryData, IConceptVectorData docData, int numberOfDocuments ) {
		m_sum = 0;
	}

	public void addConcept(
			int queryConceptId, double queryConceptScore,
			int docConceptId, double docConceptScore,
			int conceptFrequency ) {
		m_sum += queryConceptScore * docConceptScore;
	}

	public void finalizeScore( IConceptVectorData queryData, IConceptVectorData docData ) {
		m_sum = m_sum / ( queryData.getNorm2() * docData.getNorm2() );
	}

	public boolean hasScore() {
		return m_sum > 0;
	}

}
