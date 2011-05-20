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
