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

package com.talis.hbase.rdf.layout.vpindexed;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableDescLayouts;
import com.talis.hbase.rdf.layout.TupleLoaderBase;
import com.talis.hbase.rdf.store.TableDesc;
import com.talis.hbase.rdf.util.HBaseUtils;

public class TupleLoaderVPIndexed extends TupleLoaderBase
{
    protected TableDesc[] tables = null ;
    private boolean areIndexesCreated = false ;
    
	public TupleLoaderVPIndexed( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc, int chunkSize ) 
	{
		super( storeName, connection, tableDesc, chunkSize ) ;
		this.tables = tableDesc.getTables() ;
	}

	public void createTables( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		for( int i = 0 ; i < 2 ; i++ )
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( prefixAndPred[0] ) ; sb.append( "-" ) ;
			sb.append( prefixAndPred[1] ) ; sb.append( tables[i].getTableName() ) ;
			String tableName = sb.toString() ;
			if( !tables().containsKey( tableName ) ) tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
			if( row.length == 4 ) { addPredicateMapping( tableName, row[2].toString() ) ; totalSize += tableName.getBytes().length ; totalSize += row[2].toString().getBytes().length ; }
			else { addPredicateMapping( tableName, row[1].toString() ) ; totalSize += tableName.getBytes().length ; totalSize += row[1].toString().getBytes().length ; }
			tableName = null ; sb = null ;
		}
		if( !areIndexesCreated )
		{
			for( int i = 2 ; i < 5; i++ )
			{
				StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( prefixAndPred[0] ) ; sb.append( tables[i].getTableName() ) ;
				String tableName = sb.toString() ;
				if( !tables().containsKey( tableName ) ) tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
				tableName = null ; sb = null ;
			}
			areIndexesCreated = true ;
		}
		prefixAndPred = null ;
	}
	
	public void commit() throws Exception
	{
		Iterator<HTable> iterHTables = tables().values().iterator() ;
		while( iterHTables.hasNext() )
			iterHTables.next().flushCommits() ;
	}
	
	private String getPrefixAndPred( Node... row )
	{
		String prefix = null, pred = null ;
		if( row.length == 4 ) { prefix = row[0].getLocalName() ; pred = HBaseUtils.getNameOfNode( row[2] ) ; }
		else { prefix = "tbl" ; pred = HBaseUtils.getNameOfNode( row[1] ) ; }
		StringBuilder sb = new StringBuilder( prefix ) ; sb.append( "~" ) ; sb.append( pred ) ;
		return sb.toString() ;
	}
	
	public void loadTuple( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;

		for( int i = 0 ; i < 2 ; i++ )
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( prefixAndPred[0] ) ; sb.append( "-" ) ;
			sb.append( prefixAndPred[1] ) ; sb.append( tables[i].getTableName() ) ;
			HTable ht = tables().get( sb.toString() ) ; if( ht == null ) continue ;
			byte[] keyBytes = null ;
			if( i == 0 ) keyBytes = Bytes.toBytes( row[start].toString() ) ; else keyBytes = Bytes.toBytes( row[i+1+start].toString() ) ; totalSize += keyBytes.length ;
			Put update = new Put( keyBytes ) ;
			byte[] colFamilyBytes = tables[i].getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( i == 0 ) colQualBytes = Bytes.toBytes( row[2+start].toString() ) ; 
			else 
				if( start == 0 ) colQualBytes = Bytes.toBytes( row[0].toString() ) ;
				else colQualBytes = Bytes.toBytes( row[1].toString() ) ;
			totalSize += colQualBytes.length ;
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ; 
			ht.checkAndPut( keyBytes, colFamilyBytes, colQualBytes, null, update ) ; 
			update = null ; keyBytes = null ; colFamilyBytes = null ; colQualBytes = null ; sb = null ;
		}
		
		for( int i = 2 ; i < 5 ; i++ )
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( prefixAndPred[0] ) ; sb.append( tables[i].getTableName() ) ;
			HTable ht = tables().get( sb.toString() ) ; if( ht == null ) continue ;
			sb = null ; sb = new StringBuilder() ;
			if( i == 2 ) 
			{
				sb.append( row[start+2].toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; sb.append( row[start].toString() ) ; 
			}
			else if( i == 3 )
			{
				sb.append( row[start].toString() ) ;   sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; 
				sb.append( row[start+1].toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ;
				sb.append( row[start+2].toString() ) ;			
			}
			else
			{
				sb.append( row[start+2].toString() ) ;   sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; 
				sb.append( row[start].toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ;
				sb.append( row[start+1].toString() ) ;							
			}
			byte[] keyBytes = Bytes.toBytes( sb.toString() ), blankBytes = Bytes.toBytes( "" ) ; Put update = new Put( keyBytes ) ; totalSize += keyBytes.length ;
			byte[] colFamilyBytes = tables[i].getColNames().get( 0 ).getBytes() ;
			update.add( colFamilyBytes, blankBytes, blankBytes ) ; 
			ht.checkAndPut( keyBytes, colFamilyBytes, blankBytes, null, update ) ; 
			update = null ; keyBytes = null ; colFamilyBytes = null ; blankBytes = null ; sb = null ;
		}
		prefixAndPred = null ;
	}
	
	public void unloadTuple( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;

		for( int i = 0 ; i < 2 ; i++ )
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( prefixAndPred[0] ) ; sb.append( "-" ) ;
			sb.append( prefixAndPred[1] ) ; sb.append( tables[i].getTableName() ) ;
			HTable ht = tables().get( sb.toString() ) ; if( ht == null ) continue ;
			byte[] keyBytes = null ;
			if( i == 0 ) keyBytes = Bytes.toBytes( row[i+start].toString() ) ;
			else keyBytes = Bytes.toBytes( row[i+1+start].toString() ) ;
			Delete delete = new Delete( keyBytes ) ;
			byte[] colFamilyBytes = tables[i].getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( i == 0 ) colQualBytes = row[(i+2+start)%row.length].toString().getBytes() ;
			else colQualBytes = row[(i+3+start)%row.length].toString().getBytes() ;
			delete.deleteColumn( colFamilyBytes, colQualBytes ) ; ht.delete( delete ) ;
			keyBytes = null ; colFamilyBytes = null ; colQualBytes = null ; sb = null ;
		}
		
		for( int i = 2 ; i < 4 ; i ++ )
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( prefixAndPred[0] ) ; sb.append( tables[i].getTableName() ) ;
			HTable ht = tables().get( sb.toString() ) ; if( ht == null ) continue ;
			sb = null ; sb = new StringBuilder() ;
			if( i == 2 ) 
			{
				sb.append( row[start+2].toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; sb.append( row[start].toString() ) ; 
			}
			else if( i == 3 )
			{
				sb.append( row[start].toString() ) ;   sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; 
				sb.append( row[start+1].toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ;
				sb.append( row[start+2].toString() ) ;			
			}
			else
			{
				sb.append( row[start+2].toString() ) ;   sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; 
				sb.append( row[start].toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ;
				sb.append( row[start+1].toString() ) ;							
			}
			byte[] keyBytes = Bytes.toBytes( sb.toString() ), blankBytes = Bytes.toBytes( "" ) ; Delete delete = new Delete( keyBytes ) ;			
			byte[] colFamilyBytes = tables[i].getColNames().get( 0 ).getBytes() ;
			delete.deleteColumn( colFamilyBytes, blankBytes ) ; ht.delete( delete ) ;
			keyBytes = null ; colFamilyBytes = null ; blankBytes = null ; sb = null ; 
		}
		prefixAndPred = null ;
	}	
}