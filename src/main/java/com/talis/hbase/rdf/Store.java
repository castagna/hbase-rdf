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

package com.talis.hbase.rdf;

import com.hp.hpl.jena.graph.Node;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableDescLayouts;
import com.talis.hbase.rdf.store.LayoutType;
import com.talis.hbase.rdf.store.StoreConfig;
import com.talis.hbase.rdf.store.StoreFormatter;
import com.talis.hbase.rdf.store.StoreLoader;
import com.talis.hbase.rdf.store.StoreQueryRunner;

public interface Store 
{
	/** Return the store name. This is necessary in HBase since a store name provides the first point of separation over 
	 *  all HBase tables belonging to a certain store. **/
	public String 				getStoreName() ;
	
	/** Return configuration information for a store such as the graphs held within a store. **/
	public StoreConfig			getConfig() ;
	
    /** Return the connection to the implementing database */
    public HBaseRdfConnection 	getConnection() ;
    
    /** Return the query runner for the store **/
    public StoreQueryRunner		getQueryRunner() ;
    
    /** Return the processor that creates the database tables */
    public StoreFormatter   	getTableFormatter() ;
    
    /** Return the (bulk) loader */
    public StoreLoader      	getLoader() ;
    
    /** Return the layout type of the store */
    public LayoutType       	getLayoutType() ;
    
    /** Stores should be closed explicitly. 
     *  Some stores may require specific finalization actions (e.g. embedded databases),
     *  and some stores may be able to release system resources.
     */  
    public void  				close() ;

    /** Has this store been closed? **/
    public boolean 				isClosed();
    
    /** Get the size of this store **/
    public long  				getSize() ;
    
    /** Get the size of the graph corresponding to graphNode **/
    public long 				getSize( Node graphNode );    
    
    /** Where the default graph is store */ 
    public TableDescLayouts     getTablesDesc() ;    
}