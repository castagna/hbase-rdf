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

package com.talis.hbase.rdf.layout.hybrid;

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

public class TupleLoaderHybrid extends TupleLoaderBase
{
    protected TableDesc[] tables = null ;
    private boolean areTablesCreated = false ;
    
	public TupleLoaderHybrid( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc, int chunkSize ) 
	{
		super( storeName, connection, tableDesc, chunkSize ) ;
		this.tables = tableDesc.getTables() ;
	}

	public void createTables( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;

		if( !areTablesCreated )
		{
			for( int i = 0 ; i < 2 ; i++ )
			{
				String tableName = name() + "-" + prefixAndPred[0] + tables[i].getTableName() ;
				if( tables().containsKey( tableName ) ) continue ;
				else
					tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
				tableName = null ;
			}
			areTablesCreated = true ;
		}

		for( int i = 2 ; i < 4 ; i++ )
		{
			String tableName = name() + "-" + prefixAndPred[0] + "-" + prefixAndPred[1] + tables[i].getTableName() ;
			if( !tables().containsKey( tableName ) ) tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
			if( row.length == 4 ) { addPredicateMapping( tableName, row[2].toString() ) ; totalSize += tableName.getBytes().length ; totalSize += row[2].toString().getBytes().length ; }
			else { addPredicateMapping( tableName, row[1].toString() ) ; totalSize += tableName.getBytes().length ; totalSize += row[1].toString().getBytes().length ; }
			tableName = null ;
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
		return prefix + "~" + pred ;
	}
	
	public void loadTuple( Node... row ) throws Exception
	{
		totalTriples++ ;
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;

		for( int i = ( 0 + start ) ; i < row.length ; i++ )
		{
			if( ( i == 1 && start == 0 ) || ( i == 2 && start == 1 ) ) continue ;
			TableDesc desc = null ; 
			if( i == 0 && start == 0 ) desc = tables[i] ;   else if( i == 1 && start == 1 ) desc = tables[i-1] ;
			if( i == 2 && start == 0 ) desc = tables[i-1] ; else if( i == 3 && start == 1 ) desc = tables[i-2] ;
			HTable ht = tables().get( name() + "-" + prefixAndPred[0] + desc.getTableName() ) ;
			if( ht == null ) continue ;
			byte[] bytes = Bytes.toBytes( row[i].toString() ) ; Put update = new Put( bytes ) ; totalSize += bytes.length ;
			byte[] colFamilyBytes = desc.getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( ( i == 0 && start == 0 ) || ( i == 1 && start == 1 ) )
				colQualBytes = createTriple( (i+1)%(3+start), (i+2)%(3+start), row ).getBytes() ;
			else
				colQualBytes = createTriple( (i+1+start)%(3+start), (i+2+start)%(3+start), row ).getBytes() ;
			totalSize += colQualBytes.length ;
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ;
			ht.checkAndPut( bytes, colFamilyBytes, colQualBytes, null, update ) ;
			update = null ; bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}

		for( int i = 0 ; i < 2 ; i++ )
		{
			HTable ht = tables().get( name() + "-" + prefixAndPred[0] + "-" + prefixAndPred[1] + tables[i+2].getTableName() ) ;
			if( ht == null ) continue ;
			byte[] bytes = null ;
			if( i == 0 ) bytes = Bytes.toBytes( row[start].toString() ) ; else bytes = Bytes.toBytes( row[i+1+start].toString() ) ; totalSize += bytes.length ;
			Put update = new Put( bytes ) ;
			byte[] colFamilyBytes = tables[i+2].getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( i == 0 ) colQualBytes = Bytes.toBytes( row[2+start].toString() ) ; 
			else 
				if( start == 0 ) colQualBytes = Bytes.toBytes( row[0].toString() ) ;
				else colQualBytes = Bytes.toBytes( row[1].toString() ) ;
			totalSize += colQualBytes.length ;
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ; 
			ht.checkAndPut( bytes, colFamilyBytes, colQualBytes, null, update ) ; 
			update = null ; bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}
		prefixAndPred = null ;
	}
	
	public void unloadTuple( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;

		for( int i = 0 ; i < 2 ; i++ )
		{
			HTable ht = tables().get( name() + "-" + prefixAndPred[0] + "-" + prefixAndPred[1] + tables[i].getTableName() ) ;
			if( ht == null ) continue ;
			byte[] bytes = null ;
			if( i == 0 ) bytes = Bytes.toBytes( row[i+start].toString() ) ;
			else bytes = Bytes.toBytes( row[i+1+start].toString() ) ;
			Delete delete = new Delete( bytes ) ;
			byte[] colFamilyBytes = tables[i].getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( i == 0 ) colQualBytes = row[(i+2+start)%row.length].toString().getBytes() ;
			else colQualBytes = row[(i+3+start)%row.length].toString().getBytes() ;
			delete.deleteColumn( colFamilyBytes, colQualBytes ) ; ht.delete( delete ) ;
			bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}
		prefixAndPred = null ;
	}	
	
	private String createTriple( int i, int j, Node... row )
	{
		StringBuilder sb = new StringBuilder() ;
		sb.append( row[i].toString() ) ; sb.append( TableDescHybridCommon.TRIPLE_SEPARATOR ) ; sb.append( row[j].toString() ) ;
		return sb.toString() ;
	}
}