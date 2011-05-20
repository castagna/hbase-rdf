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
import java.util.Iterator;
import java.util.List;

public class IteratorOfIterators<T> implements Iterator<T> 
{
	private Iterator<T> currentIter = null ;
	private List<Iterator<T>> arr ;
	private Iterator<Iterator<T>> arrIter ;
	
	public IteratorOfIterators() { arr = new ArrayList<Iterator<T>>() ; }
	
	public void add( Iterator<T> e ) { arr.add( e ) ; }
	
	public void closeArr() { arrIter = arr.iterator() ; }
	
	@Override
	public boolean hasNext()
	{
		if( currentIter != null && currentIter.hasNext() ) return true ;
		
		while( arrIter.hasNext() )
		{
			currentIter = arrIter.next() ;
			if( currentIter.hasNext() ) return true ;
		}
		return false ;
	}
	
	@Override
	public T next()
	{
		if( currentIter != null && currentIter.hasNext() ) return currentIter.next() ;
		
		while( arrIter.hasNext() )
		{
			currentIter = arrIter.next() ;
			if( currentIter.hasNext() ) return currentIter.next() ;
		}
		return null ;
	}
	
	@Override
	public void remove() { throw new UnsupportedOperationException() ; }
}