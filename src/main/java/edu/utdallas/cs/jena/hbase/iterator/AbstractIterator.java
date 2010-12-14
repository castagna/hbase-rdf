package edu.utdallas.cs.jena.hbase.iterator;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.graph.Triple;

public abstract class AbstractIterator extends NiceIterator
{
	private Object obj = null;
	
	@Override
	public void close() { }

	public abstract Object getNextObj() ;
	
	@Override
	public boolean hasNext() 
	{
		if( obj == null ) obj = getNextObj();
		return obj != null;
	}

	@Override
	public Object next() 
	{
		if( obj == null ) { obj = getNextObj(); if( obj == null ) { throw new NoSuchElementException(); } }
		Object objToReturn = obj;
		obj = null;
		return objToReturn;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException(); }

	@Override
	public Triple removeNext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Triple> toList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Triple> toSet() {
		// TODO Auto-generated method stub
		return null;
	}

}
