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
 */
public abstract class AbstractIterator<T> extends NiceIterator<T> {
	private T obj = null;
	
	@Override
	public void close() {}

	public abstract T _next() ;
	
	@Override
	public boolean hasNext() {
		if( obj == null ) obj = _next();
		return obj != null;
	}

	@Override
	public T next() {
		if( obj == null ) { obj = _next(); if( obj == null ) { throw new NoSuchElementException(); } }
		T result = obj; obj = null; return result;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException(); }

	@Override
	public T removeNext() { throw new UnsupportedOperationException(); }

	@Override
	public List<T> toList() {
		List<T> list = new ArrayList<T>();
		while( hasNext() ) { list.add( next() ); }
		return list;
	}

	@Override
	public Set<T> toSet() {
		Set<T> set = new HashSet<T>();
		while( hasNext() ) { set.add( next() ); }
		return set;
	}

}
