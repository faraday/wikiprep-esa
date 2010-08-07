package edu.wiki.api.concept;



public interface IConceptVector {

	public double get(int key);
	
	public void add(int key, double d);

	public void set(int key, double d);
	
	public void add( IConceptVector v );
	
	public IConceptIterator iterator();
	
	public IConceptVectorData getData();
	
	public int size();
	
	public int count();
	
	public IConceptIterator orderedIterator();
}
