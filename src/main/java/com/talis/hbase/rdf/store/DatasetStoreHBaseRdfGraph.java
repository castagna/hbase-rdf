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

import java.util.Iterator;
import java.util.List;

import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.lib.Closeable;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.update.GraphStore;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.graph.GraphHBaseRdf;
import com.talis.hbase.rdf.graph.GraphHBaseRdfBase;
import com.talis.hbase.rdf.util.StoreUtils;

public class DatasetStoreHBaseRdfGraph extends DatasetGraphCaching implements DatasetGraph, Closeable, GraphStore 
{
    final Store store ;
    Context context = null ;
    ReificationStyle style = null ;    
    
    public DatasetStoreHBaseRdfGraph( Store store, Context context, ReificationStyle style )
    {
        this( store, null, context, style ) ;
    }
    
    public DatasetStoreHBaseRdfGraph( Store store, GraphHBaseRdf graph, Context context, ReificationStyle style )
    {
        this.store = store ;
        // Force the "default" graph
        this.defaultGraph = graph ;
        this.context = context ;
        this.style = style ;
    }
    
    public Store getStore() { return store ; }
    
    public Iterator<Node> listGraphNodes() { return StoreUtils.storeGraphNames( store ) ; }
	
	@Override
	protected void _close() { store.close(); }

	@Override
	protected boolean _containsGraph( Node graphNode ) { return StoreUtils.containsGraph( store, graphNode ) ; }

	@Override
	protected Graph _createDefaultGraph() { return new GraphHBaseRdfBase( store, style ); }

	@Override
	protected Graph _createNamedGraph( Node graphNode ) { return new GraphHBaseRdfBase( store, graphNode, style ) ; }

	@Override
	public void deleteAny( Node g, Node s, Node p, Node o )
	{
		store.getConfig().removeGraph( g ) ;
		Iterator<Quad> iter = find( g, s, p, o ) ;
		List<Quad> list = Iter.toList (iter ) ;
		for ( Quad q : list )
			delete( q ) ;
	}
	
	@Override
	protected void addToDftGraph( Node s, Node p, Node o ) { Helper.addToDftGraph( this, s, p, o ) ; }

	@Override
	protected void addToNamedGraph( Node g, Node s, Node p, Node o ) { Helper.addToNamedGraph( this, g, s, p, o ) ; }

	@Override
	protected void deleteFromDftGraph( Node s, Node p, Node o ) { Helper.deleteFromDftGraph( this, s, p, o ) ; }

	@Override
	protected void deleteFromNamedGraph( Node g, Node s, Node p, Node o ) { Helper.deleteFromNamedGraph( this, g, s, p, o ); }

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs( Node s, Node p, Node o ) { return Helper.findInAnyNamedGraphs( this, s, p, o ) ; }
	
	@Override
	protected Iterator<Quad> findInDftGraph( Node s, Node p, Node o ) { return Helper.findInDftGraph( this, s, p, o ) ; }

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph( Node g, Node s, Node p, Node o ) { return Helper.findInSpecificNamedGraph( this, g, s, p, o ) ; }

	@Override
	public void finishRequest() {}

	@Override
	public void startRequest() {}

	@Override
	public Dataset toDataset() { return new DatasetImpl( this ) ; }
}