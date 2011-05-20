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

package com.talis.hbase.rdf.layout;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.store.StoreInformationHolder;
import com.talis.hbase.rdf.store.StoreQueryRunner;
import com.talis.hbase.rdf.store.TableQueryRunner;

public class QueryRunnerBase extends StoreInformationHolder implements StoreQueryRunner 
{
	TableQueryRunner tblQueryRunner = null ;
	
	public QueryRunnerBase( String storeName, HBaseRdfConnection connection, Class<? extends TableQueryRunner> tableQueryRunner ) 
	{ 
		super( storeName, connection ) ; 
		try { tblQueryRunner = tableQueryRunner.getConstructor( String.class, HBaseRdfConnection.class ).newInstance( storeName, connection ) ; }
		catch( Exception e ) { throw new HBaseRdfException( "problem making tablequeryrunner", e ) ; }
	}
	
	public ExtendedIterator<Triple> storeFind( TripleMatch tm, Node graphNode )
	{
		//Create a Null iterator
		ExtendedIterator<Triple> trIter = NullIterator.instance() ;
	
		//Get the matching subject, predicate and object from the triple
		Node sm = tm.getMatchSubject(), pm = tm.getMatchPredicate(), om = tm.getMatchObject() ;
		if( sm == null ) sm = Node.ANY ; if( pm == null ) pm = Node.ANY ; if( om == null ) om = Node.ANY ;  

		try 
		{
			String prefix = ( graphNode == Quad.defaultGraphNodeGenerated ) ? "tbl" : graphNode.getLocalName() ;

			if( sm.isConcrete() ) trIter = tblQueryRunner.tableRunner( sm, pm, om, prefix, "sub" ) ;
			else
				if( om.isConcrete() ) trIter = tblQueryRunner.tableRunner( sm, pm, om, prefix, "obj" ) ;
				else
					if( pm.isConcrete() ) trIter = tblQueryRunner.tableRunner( sm, pm, om, prefix, "pred" ) ;
					else
						trIter = tblQueryRunner.tableRunner( sm, pm, om, prefix, "all" ) ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in searching triples: ", e ) ; }
		return trIter ;
	}
}