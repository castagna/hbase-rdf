/*
 * Copyright © 2010 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.hbase.rdf.iterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.hp.hpl.jena.util.iterator.NiceIterator;

/**
 * An abstract class for the iterators used.
 * @author vaibhav
 *
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractIterator extends NiceIterator
{
	/** An object in the iterator **/
	private Object obj = null;
	
	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#close()
	 */
	@Override
	public void close() { }

	/** The abstract method that will be implemented by sub-classes to return the next object in the iterator **/
	public abstract Object getNextObj() ;
	
	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#hasNext()
	 */
	@Override
	public boolean hasNext() 
	{
		if( obj == null ) obj = getNextObj();
		return obj != null;
	}

	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#next()
	 */
	@Override
	public Object next() 
	{
		if( obj == null ) { obj = getNextObj(); if( obj == null ) { throw new NoSuchElementException(); } }
		Object objToReturn = obj; obj = null; return objToReturn;
	}

	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#remove()
	 */
	@Override
	public void remove() { throw new UnsupportedOperationException(); }

	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#removeNext()
	 */
	@Override
	public Object removeNext() { throw new UnsupportedOperationException(); }

	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#toList()
	 */
	@Override
	public List<Object> toList() 
	{
		List<Object> listObj = new ArrayList<Object>();
		while( hasNext() ) { listObj.add( next() ); }
		return listObj;
	}

	/**
	 * @see com.hp.hpl.jena.util.iterator.NiceIterator#toSet()
	 */
	@Override
	public Set<Object> toSet() 
	{
		Set<Object> setObj = new HashSet<Object>();
		while( hasNext() ) { setObj.add( next() ); }
		return setObj;
	}
}
