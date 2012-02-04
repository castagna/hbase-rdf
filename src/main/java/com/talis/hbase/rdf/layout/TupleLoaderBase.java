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

package com.talis.hbase.rdf.layout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Node;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;

public abstract class TupleLoaderBase extends com.talis.hbase.rdf.store.TupleLoaderBase implements TupleLoaderBasics
{
    private static final Logger LOG = Logger.getLogger( TupleLoaderBase.class );
    protected int chunkSize ;
    protected boolean amLoading ; // flag for whether we're loading or deleting
    protected int tupleNum ;

	public TupleLoaderBase( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc, int chunkSize ) 
	{
		super( storeName, connection, tableDesc ) ;
		this.chunkSize = chunkSize ;
		this.amLoading = true ;
		this.tupleNum = 0 ;
	}

	public void load( Node... row ) 
	{
		if ( !amLoading ) { flush() ; amLoading = true ; }

		if( row.length < 3 || row.length > 4 ) throw new IllegalArgumentException( "Tuple size mismatch" );

		try { createTables( row ) ; }
		catch( Exception e ) { throw new HBaseRdfException( "Problem creating tables", e ) ; }
				
		try { loadTuple( row ) ; }
		catch( Exception e ) { throw new HBaseRdfException( "Problem adding triples", e ) ; }
		
		tupleNum++ ;
		if( tupleNum >= chunkSize ) flush() ;
	}

	public void unload( Node... row ) 
	{
		if( amLoading ) { flush() ; amLoading = false ; }
		
		if( row.length == 0 || row.length == 1 ) { massDelete( row ) ; return ; }

		if( row.length == 2 || row.length > 4 ) throw new IllegalArgumentException( "Tuple size mismatch" );

		try { unloadTuple( row ) ; }
		catch( Exception e ) { throw new HBaseRdfException( "Problem deleting triples", e ) ; }
		
		tupleNum++ ;
		if( tupleNum >= chunkSize ) flush() ;
	}
	
	private void massDelete( Node... row ) 
	{
		try 
		{
			flush() ;
			String prefix = ( row.length == 0 ) ? "tbl" : row[0].getLocalName() ;

			List<String> deletedTblNames = new ArrayList<String>() ;
			Iterator<String> iterTblNames = tables().keySet().iterator() ;
			while( iterTblNames.hasNext() )
			{
				String tblName = iterTblNames.next() ;
				if( tblName.contains( name() ) && tblName.contains( prefix ) ) 
				{ removePredicateMapping( tblName ) ; connection().deleteTable( tblName ) ; deletedTblNames.add( tblName ) ; }
			}
			for( int i = 0; i < deletedTblNames.size(); i++ )
				tables().remove( deletedTblNames.get( i ) ) ;
			deletedTblNames = null ; prefix = null ;
		} 
		catch( Exception e ) { throw new HBaseRdfException( "Exception mass deleting", e ) ; }
	}

	@Override
	public void finish() { super.finish() ; flush() ; }
	
	@Override
	public void close() 
	{ 
		super.close(); 
		try { commit() ; } 
		catch( Exception e ) { throw new HBaseRdfException( "Exception flushing in close()", e ) ; } 
	}
	
	/**
	 * Flushes all commits over all HTables. Note that delete in our case is similar to add in the sense that both involve a Put operation
	 * over a HTable.
	 */
	protected void flush() 
	{
		if( tupleNum == 0 ) return ;		
		try 
		{
			if( tupleNum % 100000 == 0 ) LOG.info( "Store flush::Checking triples added::" + tupleNum ) ;
			if( amLoading && tupleNum >= chunkSize )
			{
				LOG.info( "Store flush::Reached chunk limit" ) ;
				commit() ;
			}
		} 
		catch( Exception e ) { throw new HBaseRdfException( "Exception flushing", e ) ; } 
		finally { if( tupleNum >= chunkSize ) tupleNum = 0 ; }
	}
}