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

import java.util.Set;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.graph.query.SimpleQueryHandler;
import com.hp.hpl.jena.util.CollectionFactory;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;
import com.talis.hbase.rdf.store.GraphHBase;

public class QueryHandlerHBase extends SimpleQueryHandler implements QueryHandler
{
	Graph graph ;
	
	public QueryHandlerHBase( GraphHBase graph ) { super( graph ); this.graph = graph; }

	@Override
	public boolean containsNode( Node n ) { return super.containsNode( n ) ; }

	@Override
	public ExtendedIterator<Node> subjectsFor( Node p, Node o ) { return subjectsFor( graph, p, o ); } 

	public static ExtendedIterator<Node> subjectsFor( Graph g, Node p, Node o )
	{
		Set<Node> subjects = CollectionFactory.createHashedSet();
		ExtendedIterator<Triple> iter = g.find( Node.ANY, p, o ) ;
		while( iter.hasNext() ) subjects.add( iter.next().getSubject() );
		return WrappedIterator.createNoRemove( subjects.iterator() );		
	}
	
	@Override
	public ExtendedIterator<Node> predicatesFor( Node s, Node o ) { return predicatesFor( graph, s, o ); }
	
	public static ExtendedIterator<Node> predicatesFor( Graph g, Node s, Node o ) 
	{ 
		Set<Node> predicates = CollectionFactory.createHashedSet();
		ExtendedIterator<Triple> iter = g.find( s, Node.ANY, o ) ;
		while( iter.hasNext() ) predicates.add( iter.next().getPredicate() );
		return WrappedIterator.createNoRemove( predicates.iterator() );
	}

	@Override
	public ExtendedIterator<Node> objectsFor( Node s, Node p ) { return objectsFor( graph, s, p ); }
	
	public static ExtendedIterator<Node> objectsFor( Graph g, Node s, Node p ) 
	{ 
		Set<Node> objects = CollectionFactory.createHashedSet();
		ExtendedIterator<Triple> iter = g.find( s, p, Node.ANY ) ;
		while( iter.hasNext() ) objects.add( iter.next().getObject() );
		return WrappedIterator.createNoRemove( objects.iterator() );
	}
}
