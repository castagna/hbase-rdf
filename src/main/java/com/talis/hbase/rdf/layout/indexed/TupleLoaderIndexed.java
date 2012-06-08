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

package com.talis.hbase.rdf.layout.indexed;

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

public class TupleLoaderIndexed extends TupleLoaderBase
{
    protected TableDesc[] tables = null ;
    private boolean areTablesCreated = false ;
    
	public TupleLoaderIndexed( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc, int chunkSize ) 
	{
		super( storeName, connection, tableDesc, chunkSize ) ;
		this.tables = tableDesc.getTables() ;
	}

	public void createTables( Node... row ) throws Exception
	{
		if( areTablesCreated ) return ;
		String prefix = getPrefix( row ) ;
		for( int i = 0 ; i < tables.length ; i++ )
		{
			String tableName = name() + "-" + prefix + tables[i].getTableName() ;
			if( tables().containsKey( tableName ) ) continue ;
			else
				tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
			tableName = null ;
		}
		prefix = null ; areTablesCreated = true ;
	}
	
	public void commit() throws Exception
	{
		Iterator<HTable> iterHTables = tables().values().iterator() ;
		while( iterHTables.hasNext() )
			iterHTables.next().flushCommits() ;
	}
	
	public void loadTuple( Node... row ) throws Exception
	{
		String prefix = getPrefix( row ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;
		
		for( int i = ( 0 + start ) ; i < row.length ; i++ )
		{
			for( int j = 2*i ; j <= ( 2*i + 1 ) ; j ++ )
			{
				TableDesc desc = null ; if( start == 0 ) desc = tables[j] ; else desc = tables[j-2] ;
				HTable ht = tables().get( name() + "-" + prefix + desc.getTableName() ) ;
				if( ht == null ) continue ;
				StringBuilder sb = new StringBuilder( row[i].toString() ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ;
				if( ( i == 1 && start == 0 ) || ( i == 2 && start == 1 ) )
				{
					if( j == 2 || j == 4 ) { sb.append( row[(i-1)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+1)%(3+start)] ) ; }
					else if( j == 3 || j == 5 ) { sb.append( row[(i+1)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i-1)%(3+start)] ) ; }
				}
				else
					if( ( i == 0 && start == 0 ) || ( i == 1 && start == 1 ) )
					{
						if( j == 0 || j == 2 ) { sb.append( row[(i+1)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+2)%(3+start)] ) ; }
						else if( j == 1 || j == 3 ) { sb.append( row[(i+2)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+1)%(3+start)] ) ; }
					}
					else
					{
						if( j == 4 || j == 6 ) { sb.append( row[(i+1+start)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+2+start)%(3+start)] ) ; }
						else if( j == 5 || j == 7 ) { sb.append( row[(i+2+start)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+1+start)%(3+start)] ) ; }
					}
				byte[] bytes = Bytes.toBytes( sb.toString() ) ;	Put update = new Put( bytes ) ; totalSize += bytes.length ;
				byte[] colFamilyBytes = desc.getColNames().get( 0 ).getBytes() ;
				update.add( colFamilyBytes, Bytes.toBytes( "" ), Bytes.toBytes( "" ) ) ;
				ht.checkAndPut( bytes, colFamilyBytes, Bytes.toBytes( "" ), null, update ) ;
				update = null ; bytes = null ; colFamilyBytes = null ;
			}
		}
		prefix = null ;
	}
	
	public void unloadTuple( Node... row ) throws Exception
	{
		String prefix = getPrefix( row ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;
		for( int i = ( 0 + start ) ; i < row.length ; i++ )
		{
			for( int j = 2*i ; j <= ( 2*i + 1 ) ; j ++ )
			{
				TableDesc desc = null ; if( start == 0 ) desc = tables[j] ; else desc = tables[j-2] ;
				HTable ht = tables().get( name() + "-" + prefix + desc.getTableName() ) ;
				if( ht == null ) continue ;
				StringBuilder sb = new StringBuilder( row[i].toString() ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ;
				if( ( i == 1 && start == 0 ) || ( i == 2 && start == 1 ) )
				{
					if( j == 2 || j == 4 ) { sb.append( row[(i-1)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+1)%(3+start)] ) ; }
					else if( j == 3|| j == 5 ) { sb.append( row[(i+1)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i-1)%(3+start)] ) ; }
				}
				else
					if( ( i == 0 && start == 0 ) || ( i == 1 && start == 1 ) )
					{
						if( j == 0 || j == 2 ) { sb.append( row[(i+1)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+2)%(3+start)] ) ; }
						else if( j == 1 || j == 3 ) { sb.append( row[(i+2)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+1)%(3+start)] ) ; }
					}
					else
					{
						if( j == 4 || j == 6 ) { sb.append( row[(i+1+start)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+2+start)%(3+start)] ) ; }
						else if( j == 5 || j == 7 ) { sb.append( row[(i+2+start)%(3+start)] ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( row[(i+1+start)%(3+start)] ) ; }
					}
				byte[] bytes = Bytes.toBytes( sb.toString() ) ;	Delete delete = new Delete( bytes ) ; 
				byte[] colFamilyBytes = desc.getColNames().get( 0 ).getBytes() ;
				delete.deleteColumn( colFamilyBytes, Bytes.toBytes( "" ) ) ; ht.delete( delete ) ;
				bytes = null ; colFamilyBytes = null ;
			}
		}
		prefix = null ;
	}
	
	private String getPrefix( Node... row ) { return ( row.length == 4 ) ? row[0].getLocalName() : "tbl" ; }
}