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

package com.talis.hbase.rdf.layout.hash;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sdb.layout2.NodeLayout2;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableDescLayouts;
import com.talis.hbase.rdf.layout.TupleLoaderBase;
import com.talis.hbase.rdf.store.TableDesc;
import com.talis.hbase.rdf.util.HBaseUtils;

public class TupleLoaderHash extends TupleLoaderBase
{
    protected TableDesc[] tables = null ;
    private boolean areTablesCreated = false ;
    
	public TupleLoaderHash( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc, int chunkSize ) 
	{
		super( storeName, connection, tableDesc, chunkSize ) ;
		this.tables = tableDesc.getTables() ;
	}

	public void createTables( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;

		if( !areTablesCreated )
		{
			String nodeTblName = name() + "-" + prefixAndPred[0] + tables[0].getTableName() ;
			tables().put( nodeTblName, connection().createTable( nodeTblName, tables[0].getColNames() ) ) ;
			for( int i = 1 ; i < 3 ; i++ )
			{
				String tableName = name() + "-" + prefixAndPred[0] + tables[i].getTableName() ;
				if( tables().containsKey( tableName ) ) continue ;
				else
					tables().put( tableName, connection().createTable( tableName, tables[i].getColNames() ) ) ;
				tableName = null ;
			}
			areTablesCreated = true ;
		}

		for( int i = 3 ; i < 5 ; i++ )
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
	
	private byte[] createTriple( byte[] hash1Bytes, byte[] sepBytes, byte[] hash2Bytes )
	{
		byte[] tripleBytes = new byte[hash1Bytes.length + sepBytes.length + hash2Bytes.length] ;
		int x = 0 ;
		for( int i = 0 ; i < hash1Bytes.length ; i++ ) tripleBytes[x++] = hash1Bytes[i] ;
		for( int i = 0 ; i < sepBytes.length ; i++ )   tripleBytes[x++] = sepBytes[i] ;
		for( int i = 0 ; i < hash2Bytes.length ; i++ ) tripleBytes[x++] = hash2Bytes[i] ;
		return tripleBytes ;
	}
	
	public void loadTuple( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;

		for( int i = ( 0 + start ) ; i < row.length ; i++ )
		{
			if( ( i == 1 && start == 0 ) || ( i == 2 && start == 1 ) ) continue ;
			TableDesc desc = null ; 
			if( i == 0 && start == 0 ) desc = tables[i+1] ;   else if( i == 1 && start == 1 ) desc = tables[i-1+1] ;
			if( i == 2 && start == 0 ) desc = tables[i-1+1] ; else if( i == 3 && start == 1 ) desc = tables[i-2+1] ;
			HTable ht = tables().get( name() + "-" + prefixAndPred[0] + desc.getTableName() ) ;
			if( ht == null ) continue ;
			long hash = NodeLayout2.hash( row[i] ) ; addNodeToNodesTable( row[i], hash, row ) ; byte[] bytes = Bytes.toBytes( hash ) ; Put update = new Put( bytes ) ;
			totalSize += bytes.length ;
			
			byte[] colFamilyBytes = desc.getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( ( i == 0 && start == 0 ) || ( i == 1 && start == 1 ) )
			{
				long hash1 = NodeLayout2.hash( row[(i+1)%(3+start)] ), hash2 = NodeLayout2.hash( row[(i+2)%(3+start)] ) ;
				addNodeToNodesTable( row[(i+1)%(3+start)], hash1, row ) ; addNodeToNodesTable( row[(i+2)%(3+start)], hash2, row ) ;
				byte[] hash1Bytes = Bytes.toBytes( hash1 ), hash2Bytes = Bytes.toBytes( hash2 ), sepBytes = Bytes.toBytes( TableDescHashCommon.TRIPLE_SEPARATOR ) ;
				colQualBytes = createTriple( hash1Bytes, sepBytes, hash2Bytes ) ;
				totalSize += colQualBytes.length ;
			}
			else
			{
				long hash1 = NodeLayout2.hash( row[(i+1+start)%(3+start)] ), hash2 = NodeLayout2.hash( row[(i+2+start)%(3+start)] ) ;
				//addNodeToNodesTable( row[(i+1+start)%(3+start)], hash1, row ) ; addNodeToNodesTable( row[(i+2+start)%(3+start)], hash2, row ) ;
				byte[] hash1Bytes = Bytes.toBytes( hash1 ), hash2Bytes = Bytes.toBytes( hash2 ), sepBytes = Bytes.toBytes( TableDescHashCommon.TRIPLE_SEPARATOR ) ;
				colQualBytes = createTriple( hash1Bytes, sepBytes, hash2Bytes ) ;
				totalSize += colQualBytes.length ;
			}
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ;
			ht.checkAndPut( bytes, colFamilyBytes, colQualBytes, null, update ) ;
			update = null ; bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}

		for( int i = 0 ; i < 2 ; i++ )
		{
			HTable ht = tables().get( name() + "-" + prefixAndPred[0] + "-" + prefixAndPred[1] + tables[i+3].getTableName() ) ;
			if( ht == null ) continue ;
			byte[] bytes = null ; long hash = 0L ;
			if( i == 0 ) { hash = NodeLayout2.hash( row[start] ) ; bytes = Bytes.toBytes( hash ) ; } //addNodeToNodesTable( row[start], hash, row ) ; }
			else { hash = NodeLayout2.hash( row[i+1+start] ) ; bytes = Bytes.toBytes( hash ) ; } //addNodeToNodesTable( row[i+1+start], hash, row ) ; }
			totalSize += bytes.length ;
			
			Put update = new Put( bytes ) ;
			byte[] colFamilyBytes = tables[i+3].getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( i == 0 ) { hash = NodeLayout2.hash( row[2+start] ) ; colQualBytes = Bytes.toBytes( hash ) ; } //addNodeToNodesTable( row[2+start], hash, row ) ; }
			else 
				if( start == 0 ) { hash = NodeLayout2.hash( row[0] ) ; colQualBytes = Bytes.toBytes( hash ) ; } //addNodeToNodesTable( row[0], hash, row ) ; }
				else { hash = NodeLayout2.hash( row[1] ) ; colQualBytes = Bytes.toBytes( hash ) ; } //addNodeToNodesTable( row[1], hash, row ) ; }
			totalSize += colQualBytes.length ;
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ; 
			ht.checkAndPut( bytes, colFamilyBytes, colQualBytes, null, update ) ; 
			update = null ; bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}
		prefixAndPred = null ;
	}
	
	private void addNodeToNodesTable( Node n, long hash, Node... row ) throws IOException
	{
		HTable ht = tables().get( name() + "-" + getPrefixAndPred( row ).split( "~" )[0] + tables[0].getTableName() ) ;
		byte[] bytes = Bytes.toBytes( hash ), colFamilyBytes = tables[0].getColNames().get( 0 ).getBytes(), colQualBytes = n.toString().getBytes() ;
		totalSize += bytes.length ; totalSize += colQualBytes.length ;
		Put update = new Put( bytes ) ;
		update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ;
		ht.checkAndPut( bytes, colFamilyBytes, colQualBytes, null, update ) ;
	}
	
	public void unloadTuple( Node... row ) throws Exception
	{
		String[] prefixAndPred = getPrefixAndPred( row ).split( "~" ) ;
		int start = ( row.length == 3 ) ? 0 : 1 ;

		for( int i = 0 ; i < 2 ; i++ )
		{
			HTable ht = tables().get( name() + "-" + prefixAndPred[0] + "-" + prefixAndPred[1] + tables[i].getTableName() ) ;
			if( ht == null ) continue ;
			long hash = 0L ; byte[] bytes = null ;
			if( i == 0 ) { hash = NodeLayout2.hash( row[i+start] ) ; bytes = Bytes.toBytes( "" + hash ) ; }
			else { hash = NodeLayout2.hash( row[i+1+start] ) ; bytes = Bytes.toBytes( "" + hash ) ; }
			Delete delete = new Delete( bytes ) ;
			byte[] colFamilyBytes = tables[i].getColNames().get( 0 ).getBytes(), colQualBytes = null ;
			if( i == 0 ) { hash = NodeLayout2.hash( row[(i+2+start)%row.length] ) ; colQualBytes = Bytes.toBytes( "" + hash ) ; }
			else { hash = NodeLayout2.hash( row[(i+3+start)%row.length] ) ; colQualBytes = Bytes.toBytes( "" + hash ) ; }
			delete.deleteColumn( colFamilyBytes, colQualBytes ) ; ht.delete( delete ) ;
			bytes = null ; colFamilyBytes = null ; colQualBytes = null ;
		}
		prefixAndPred = null ;
	}		
}