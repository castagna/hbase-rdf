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

package com.talis.hbase.rdf.layout.simple;

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

public class TupleLoaderSimple extends TupleLoaderBase
{
    protected TableDesc[] tables = null ;

	public TupleLoaderSimple( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc, int chunkSize ) 
	{
		super( storeName, connection, tableDesc, chunkSize ) ;
		this.tables = tableDesc.getTables() ;
	}

	public void createTables( Node... row ) throws Exception
	{
		String prefix = getPrefix( row ) ;
		for( int i = 0 ; i < tables.length ; i++ )
		{
			String tableName = name() + "-" + prefix + tables[i].getTableName() ;
			if( !connection().getAdmin().tableExists( tableName ) )
				tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
			else if( !tables().containsKey( tableName ) )
				tables().put( tableName, connection().openTable( tableName ) ) ;
			tableName = null ;
		}
		prefix = null ; 
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
			TableDesc desc = null ; if( start == 0 ) desc = tables[i] ; else desc = tables[i-1] ;
			HTable ht = tables().get( name() + "-" + prefix + desc.getTableName() ) ;
			if( ht == null ) continue ;
			byte[] bytes = Bytes.toBytes( row[i].toString() ) ; Put update = new Put( bytes ) ;
			byte[] colFamilyBytes = desc.getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( ( i == 1 && start == 0 ) || ( i == 2 && start == 1 ) )
				colQualBytes = createTriple( (i-1)%(3+start), (i+1)%(3+start), row ).getBytes() ;
			else
				if( ( i == 0 && start == 0 ) || ( i == 1 && start == 1 ) )
					colQualBytes = createTriple( (i+1)%(3+start), (i+2)%(3+start), row ).getBytes() ;
				else
					colQualBytes = createTriple( (i+1+start)%(3+start), (i+2+start)%(3+start), row ).getBytes() ;
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ;
			ht.checkAndPut( bytes, colFamilyBytes, colQualBytes, null, update ) ;
			update = null ; bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}
		prefix = null ;
	}
	
	public void unloadTuple( Node... row ) throws Exception
	{
		String prefix = getPrefix( row ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;
		for( int i = ( 0 + start ) ; i < row.length ; i++ )
		{
			TableDesc desc = null ; if( start == 0 ) desc = tables[i] ; else desc = tables[i-1] ;
			HTable ht = tables().get( name() + "-" + prefix + desc.getTableName() ) ;
			if( ht == null ) continue ;
			byte[] bytes = Bytes.toBytes( row[i].toString() ) ; Delete delete = new Delete( bytes ) ;
			byte[] colFamilyBytes = desc.getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( ( i == 1 && start == 0 ) || ( i == 2 && start == 1 ) )
				colQualBytes = createTriple( (i-1)%(3+start), (i+1)%(3+start), row ).getBytes() ;
			else if( ( i == 0 && start == 0 ) || ( i == 1 && start == 1 ) )
				colQualBytes = createTriple( (i+1)%(3+start), (i+2)%(3+start), row ).getBytes() ;
			else
				colQualBytes = createTriple( (i+1+start)%(3+start), (i+2+start)%(3+start), row ).getBytes() ;
			delete.deleteColumn( colFamilyBytes, colQualBytes ) ; ht.delete( delete ) ;
			bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}
		prefix = null ;
	}
	
	private String getPrefix( Node... row ) { return ( row.length == 4 ) ? row[0].getLocalName() : "tbl" ; }

	private String createTriple( int i, int j, Node... row )
	{
		StringBuilder sb = new StringBuilder() ;
		sb.append( row[i].toString() ) ; sb.append( TableDescSimpleCommon.TRIPLE_SEPARATOR ) ; sb.append( row[j].toString() ) ;
		return sb.toString() ;
	}
}