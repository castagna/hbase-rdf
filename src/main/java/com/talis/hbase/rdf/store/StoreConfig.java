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

package com.talis.hbase.rdf.store;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;

public class StoreConfig extends StoreInformationHolder
{
	protected Map<String, Node> graphURIsInStore = new HashMap<String, Node>() ;
	protected Map<String, Graph> graphsInStore = new HashMap<String, Graph>() ;

	public StoreConfig( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	public void addGraphInfoToStore( Node graphNode, Graph graph ) 
	{ 
		if( ! graphNode.hasURI( "urn:x-arq:UnionGraph" ) )
		{
			graphURIsInStore.put( graphNode.getLocalName(), graphNode ); 
			graphsInStore.put( graphNode.getLocalName(), graph ) ;
		}
	}
	
	public Iterator<Node> listGraphNodes() 			{ return graphURIsInStore.values().iterator() ; }
	
	public boolean containsGraph( Node graphNode ) 	{ return graphURIsInStore.containsValue( graphNode ) ; }

	public Iterator<Graph> listGraphs() 			{ return graphsInStore.values().iterator() ; }
	
	public void removeGraph( Node graphNode ) 		{ String key = graphNode.getLocalName() ; graphURIsInStore.remove( key ) ; graphsInStore.remove( key ) ; key = null ; }
}
