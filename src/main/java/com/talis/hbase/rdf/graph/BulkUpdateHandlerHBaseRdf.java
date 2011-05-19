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

package com.talis.hbase.rdf.graph;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.SimpleBulkUpdateHandler;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class BulkUpdateHandlerHBaseRdf extends SimpleBulkUpdateHandler implements BulkUpdateHandler 
{
	public BulkUpdateHandlerHBaseRdf( GraphHBaseRdfBase graph ) 
	{ super( graph ) ; }

	public void removeAll()
	{ removeAll( graph ); notifyRemoveAll() ; }

	protected void notifyRemoveAll()
	{ manager.notifyEvent( graph, GraphEvents.removeAll ) ; }

	public void remove( Node s, Node p, Node o )
	{ removeAll( graph, s, p, o ) ; manager.notifyEvent( graph, GraphEvents.remove( s, p, o ) ) ; }

	public static void removeAll( Graph g, Node s, Node p, Node o )
	{
		ExtendedIterator<Triple> it = g.find( s, p, o ) ;
		try { while ( it.hasNext()) { g.delete( it.next() ) ; } }
		finally { it.close() ; }
	}

	public static void removeAll( Graph g )
	{
		ExtendedIterator<Triple> it = GraphUtil.findAll( g ) ;
		try { while ( it.hasNext() ) { g.delete( it.next() ) ; } }
		finally { it.close() ; }
	}
}