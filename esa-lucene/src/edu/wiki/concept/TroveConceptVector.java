package edu.wiki.concept;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.IConceptVectorData;
import gnu.trove.TDoubleProcedure;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;

import java.io.Serializable;

public class TroveConceptVector implements IConceptVector, Serializable {

	private static final long serialVersionUID = 5228670885044409972L;

	private TIntDoubleHashMap valueMap;
	private int size;
	
	public TroveConceptVector(int size ) {
		this.size = size;
		valueMap = new TIntDoubleHashMap(size);		
	}
	
	@Override
	public void add(int key, double d) {
		valueMap.put( key, valueMap.get( key ) + d );
	}

	@Override
	public void add( IConceptVector v ) {
		IConceptIterator it = v.iterator();
		while( it.next() ) {
			add( it.getId(), it.getValue() );
		}
	}

	@Override
	public int count() {
		return valueMap.size();
	}

	@Override
	public double get( int key ) {
		return valueMap.get( key );
	}

	@Override
	public IConceptVectorData getData() {
		return new TroveConceptVectorData();
	}

	@Override
	public IConceptIterator iterator() {
		return new TroveConceptVectorIterator();
	}

	@Override
	public IConceptIterator orderedIterator() {
		return new TroveConceptVectorOrderedIterator( valueMap );
	}

	@Override
	public void set( int key, double d ) {
		if( d != 0 ) {
			valueMap.put( key, d );
		}
		else {
			valueMap.remove( key );
		}
	}

	@Override
	public int size() {
		return size;
	}

	private class TroveConceptVectorData implements IConceptVectorData {
		
		public int getConceptCount() {
			return valueMap.size();
		}
		
		public double getNorm1() {
			NormProcedure n1 = new NormProcedure( 1 );
			valueMap.forEachValue( n1 );
			return n1.getNorm();
		}
		
		public double getNorm2() {
			NormProcedure n2 = new NormProcedure( 2 );
			valueMap.forEachValue( n2 );
			return n2.getNorm();
		}
		
		private class NormProcedure implements TDoubleProcedure {

			int p;
			double sum;
			
			private NormProcedure( int p ) {
				this.p = p;
				sum = 0;
			}
			
			@Override
			public boolean execute( double value ) {
				if( p == 1 ) {
					sum += value;
				}
				else {
					sum += Math.pow( value, p );
				}
				return true;
			}
			
			private double getNorm() {
				return Math.pow( sum, 1.0 / (double)p );
			}
		}
	}
	
	private class TroveConceptVectorIterator implements IConceptIterator {

		TIntDoubleIterator valueIt;
		
		private TroveConceptVectorIterator() {
			reset();
		}
		
		@Override
		public int getId() {
			return valueIt.key();
		}

		@Override
		public double getValue() {
			return valueIt.value();
		}

		@Override
		public boolean next() {
			if( valueIt.hasNext() ) {
				valueIt.advance();
				return true;
			}
			return false;
		}

		@Override
		public void reset() {
			valueIt = valueMap.iterator();
		}
		
	}
}
