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

package com.talis.hbase.rdf.store;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.talis.hbase.rdf.shared.HBaseRdfInternalError;
import com.talis.hbase.rdf.shared.HBaseRdfNotImplemented;

public class TupleGraphLoader 
{
    private TupleLoader loader ;

    /** The loader must be for a triple table of some kind */
    public TupleGraphLoader( TupleLoader loader )
    { 
        if ( loader.getTableDesc() == null )
            throw new HBaseRdfInternalError( "No table description for loader" ) ;
        this.loader = loader ;
    }
        
    public void addTriple( Triple triple ) 		{ loader.load( row( triple ) ) ; }

    public void deleteTriple( Triple triple ) 	{ loader.unload( row( triple ) ) ; }
    
    private static Node[] row( Triple triple )
    {
        Node[] nodes = new Node[3] ;
        nodes[0] = triple.getSubject() ;
        nodes[1] = triple.getPredicate() ;
        nodes[2] = triple.getObject() ;
        return nodes ;
    }

    public void close() 								{ loader.finish() ; }

    public void startBulkUpdate() 						{ loader.start() ; }

    public void finishBulkUpdate() 						{ loader.finish() ; }

    public int getChunkSize() 							{ throw new HBaseRdfNotImplemented( "TupleGraphLoader.getChunkSize" ) ; }
    
    public void setChunkSize( int chunks ) 				{ throw new HBaseRdfNotImplemented( "TupleGraphLoader.setChunkSize" ) ; }

    public boolean getUseThreading() 					{ throw new HBaseRdfNotImplemented( "TupleGraphLoader.getUseThreading" ) ; }

    public void setUseThreading( boolean useThreading ) { throw new HBaseRdfNotImplemented( "TupleGraphLoader.setUseThreading" ) ; }
    
	public void deleteAll() 							{ throw new HBaseRdfNotImplemented( "TupleGraphLoader.deleteAll" ); }
}