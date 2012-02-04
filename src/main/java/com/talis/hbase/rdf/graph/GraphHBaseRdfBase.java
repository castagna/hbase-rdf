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

package com.talis.hbase.rdf.graph;

import java.util.Iterator;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Reifier;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.shared.impl.JenaParameters;
import com.hp.hpl.jena.sparql.core.DatasetPrefixStorage;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import com.hp.hpl.jena.sparql.graph.GraphBase2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.talis.hbase.rdf.HBaseRdf;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.store.DatasetStoreHBaseRdfGraph;
import com.talis.hbase.rdf.store.StoreLoaderPlus;

/**
 * A graph implementation for HBase.
 */
public class GraphHBaseRdfBase extends GraphBase2 implements GraphHBaseRdf 
{
	//private final QueryHandlerHBaseRdf queryHandler = new QueryHandlerHBaseRdf( this ) ;
	private final TransactionHandler transactionHandler = new TransactionHandlerHBaseRdf( this ) ;
	//private final BulkUpdateHandler bulkUpdateHandler = new BulkUpdateHandlerHBaseRdf( this ) ;
	//private final Reifier reifier ;
	protected final DatasetStoreHBaseRdfGraph dataset ;
	protected final Node graphNode ;
	protected Store store = null ;
	private final DatasetPrefixStorage prefixes ;
    protected int inBulkUpdate = 0 ;

    public GraphHBaseRdfBase( Store store, String uri, ReificationStyle style )
    { 
        this( store, Node.createURI( uri ), style ) ;       
    }
    
    public GraphHBaseRdfBase( Store store, ReificationStyle style )
    { 
        this( store, (Node)null, style ) ;
    }

    public GraphHBaseRdfBase( Store store, Node graphNode, ReificationStyle style )
    {
        //Add this graph to the current store
    	if( graphNode != null )
    		store.getConfig().addGraphInfoToStore( graphNode, this ) ;

        if ( graphNode == null )
            graphNode = Quad.defaultGraphNodeGenerated ;

        this.graphNode = graphNode ;
        this.store = store ;
   
		this.prefixes = new DatasetPrefixesHBaseRdf();
		this.reifier = new ReifierHBaseRdf( this, style ) ;
		
		this.queryHandler = new QueryHandlerHBaseRdf( this ) ;
		this.bulkHandler = new BulkUpdateHandlerHBaseRdf( this ) ;
		
		//Use the deterministic blank node generation algorithm
		JenaParameters.disableBNodeUIDGeneration = true ;

        // Avoid looping here : DatasetStoreGraph can make GraphSDB's
        dataset = new DatasetStoreHBaseRdfGraph( store, this, HBaseRdf.getContext().copy(), style ) ;   
        
        if( graphNode.hasURI( "urn:x-arq:UnionGraph" ) )
        	addTriplesToSpecializedGraph() ;
    }
	
    private void addTriplesToSpecializedGraph()
    {
    	Iterator<Graph> graphs = store.getConfig().listGraphs() ;
    	while( graphs.hasNext() ) getBulkUpdateHandler().add( graphs.next() ) ;
    }
    
	public void clear() { store.getTableFormatter().truncate() ; }
	
	/**
	 * @see com.hp.hpl.jena.graph.GraphAdd#add( com.hp.hpl.jena.graph.Triple )
	 */
	@Override
	public void performAdd( Triple triple ) 
	{
		//TODO: Don't know if we should be doing this, i.e. differentiating between reification styles.
		if( getReifier().getStyle().conceals() && getReifier().getStyle().intercepts() ) return ;

    	if ( inBulkUpdate == 0 ) store.getLoader().startBulkUpdate() ;
    	
        if ( Quad.isQuadDefaultGraphGenerated( graphNode ) )
            store.getLoader().addTriple(triple) ;
        else
        {
            // XXX
            StoreLoaderPlus x = (StoreLoaderPlus)store.getLoader() ;
            x.addQuad( graphNode, triple.getSubject(), triple.getPredicate(), triple.getObject() ) ;
        }
        if ( inBulkUpdate == 0 ) store.getLoader().finishBulkUpdate() ;
	}
	
	@Override
	public void performDelete( Triple triple )
	{
    	if (inBulkUpdate == 0) store.getLoader().startBulkUpdate();
        if ( Quad.isQuadDefaultGraphGenerated( graphNode ) )
            store.getLoader().deleteTriple( triple ) ;
        else
        {
            // XXX
            StoreLoaderPlus x = (StoreLoaderPlus)store.getLoader() ;
            x.deleteQuad( graphNode, triple.getSubject(), triple.getPredicate(), triple.getObject() ) ;
        }
        if (inBulkUpdate == 0) store.getLoader().finishBulkUpdate();
	}
	
	@Override
	public void close() { store.close() ; }
	
    public void startBulkUpdate()  { inBulkUpdate += 1 ; if (inBulkUpdate == 1) store.getLoader().startBulkUpdate() ; }
    public void finishBulkUpdate() { inBulkUpdate -= 1 ; if (inBulkUpdate == 0) store.getLoader().finishBulkUpdate( ) ; }

	/**
	 * @see com.hp.hpl.jena.graph.Graph#find( com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Node )
	 */
	@Override
	protected ExtendedIterator<Triple> graphBaseFind( TripleMatch tm ) { return store.getQueryRunner().storeFind( tm, graphNode ) ; }

	@Override
	public Capabilities getCapabilities() 
	{
		if ( capabilities == null )
			capabilities = new Capabilities() 
			{
				public boolean sizeAccurate() 					{ return true ; }
				public boolean addAllowed() 					{ return true ; }
				public boolean addAllowed( boolean every ) 		{ return true ; }
				public boolean deleteAllowed() 					{ return true ; }
				public boolean deleteAllowed( boolean every ) 	{ return true ; }
				public boolean canBeEmpty() 					{ return true ; }
				public boolean iteratorRemoveAllowed() 			{ return false ; }
				public boolean findContractSafe() 				{ return true ; }
				public boolean handlesLiteralTyping() 			{ return false ; }
			} ;
		return super.getCapabilities() ;
	}

	//@Override
	public final Node getGraphNode() { return graphNode ; }

	//@Override
	public final DatasetStoreHBaseRdfGraph getDataset() { return dataset ; }

	//@Override
	public Lock getLock() { return dataset.getLock() ; }

	@Override
	public BulkUpdateHandler getBulkUpdateHandler() { return bulkHandler ; }

	@Override
	public QueryHandler queryHandler() { return queryHandler ; }

	@Override
	public TransactionHandler getTransactionHandler() { return transactionHandler ; }

	@Override
	protected PrefixMapping createPrefixMapping()  { return prefixes.getPrefixMapping() ; }

	public Reifier getReifier() { return reifier ; }
	
	@Override
	public void sync() 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void sync( boolean arg0 ) 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public ReorderTransformation getReorderTransform() 
	{
		// TODO Auto-generated method stub
		return null;
	}
}