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
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.store.LayoutType;
import com.talis.hbase.rdf.store.StoreConfig;
import com.talis.hbase.rdf.store.StoreFormatter;
import com.talis.hbase.rdf.store.StoreInformationHolder;
import com.talis.hbase.rdf.store.StoreLoader;
import com.talis.hbase.rdf.store.StoreQueryRunner;

public abstract class StoreBase extends StoreInformationHolder implements Store
{
	protected StoreDesc storeDescription ;
	protected StoreConfig config ;
	protected StoreQueryRunner querier ;
	protected StoreFormatter formatter ;
	protected StoreLoader loader ;
	protected TableDescLayouts tables ;
	protected boolean isClosed = false ;

	public StoreBase( HBaseRdfConnection connection, StoreDesc desc, StoreQueryRunner querier, StoreFormatter formatter, StoreLoader loader, TableDescLayouts tables )
	{
		super( desc.getStoreName(), connection ) ;
		this.storeDescription = desc ;
		this.querier = querier ;
		this.formatter = formatter ;
		this.loader = loader ;
		this.tables = tables ;
		this.config = new StoreConfig( desc.getStoreName(), connection ) ;
	}

	public HBaseRdfConnection  	getConnection()     					{ return connection() ; }

	public StoreConfig			getConfig()								{ return config ; }
	
	public StoreQueryRunner 	getQueryRunner() 						{ return querier ; }
	
	public StoreFormatter   	getTableFormatter() 					{ return formatter ; }

	public StoreLoader      	getLoader()         					{ return loader ; }

	public LayoutType       	getLayoutType()     					{ return storeDescription.getLayout() ; }
	
	public String 				getStoreName()							{ return storeDescription.getStoreName() ; }

	public TableDescLayouts getTablesDesc() 							{ return tables ; }
	
	public void close() 												{ getLoader().close(); isClosed = true; }
	
	public boolean isClosed() 											{ return isClosed; }
	
    public long getSize() 												{ return getSize( Quad.defaultGraphNodeGenerated ) ; }
    
	public long getSize( Node node )
	{
    	long count = 0L ;
    	ExtendedIterator<Triple> iterTriplesInStore = querier.storeFind( Triple.create( Node.ANY, Node.ANY, Node.ANY ), node ) ;
    	while( iterTriplesInStore.hasNext() ) { count++ ; iterTriplesInStore.next() ; }
    	return count ;
	}
}