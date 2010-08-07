package edu.wiki.api.concept;


public interface IConceptIterator {

	public boolean next();
	
	public int getId();
	
	public double getValue();
	
	public void reset();
	
}
