/*
 * Copyright © 2010, 2011, 2012 Talis Systems Ltd.
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

package com.talis.hbase.rdf.layout.vpindexed;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Triple;
import com.talis.hbase.rdf.iterator.AbstractIterator;
import com.talis.hbase.rdf.iterator.IteratorOfIterators;

public class HBaseRdfAllTablesIterator extends AbstractIterator<Triple>
{
	private IteratorOfIterators<Triple> allIters = new IteratorOfIterators<Triple>() ;
	
	public HBaseRdfAllTablesIterator() { }
	
	public void addIter( Iterator<Triple> baseIter ) { allIters.add( baseIter ) ; } 
	
	public void closeIter() { allIters.closeArr() ; }
	
	public Triple _next() { return allIters.next() ; }
}
