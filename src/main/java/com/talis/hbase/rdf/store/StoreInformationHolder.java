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

package com.talis.hbase.rdf.store;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;

public class StoreInformationHolder
{
	private String name ;
    private HBaseRdfConnection conn ;
    private static Map<String, HTable> hTables = new HashMap<String, HTable>() ;
    private static HTable predMappingTbl = null ;
    private static final String predURIColFam = "predicateURIs" ;
    public static long totalSize = 0L ; 
    public static long totalTriples = 0L ;
    
    protected StoreInformationHolder( String name, HBaseRdfConnection connection ) 
    { 
    	this.name = name ; this.conn = connection ; 
    	
    	if( hTables.isEmpty() )
    	{
	    	try
	    	{
		    	//Fill the map with tables that belong to this store
		    	HTableDescriptor[] tblDescs = connection.getAdmin().listTables() ;
		    	for( int i = 0 ; i < tblDescs.length; i++ )
		    	{
		    		String tblDesc = tblDescs[i].getNameAsString() ;
		    		if( tblDesc.equals( name + "-predicate-mapping" ) ) continue ;
		    		String[] splitStoreName = tblDesc.split( name ) ;
		    		if( splitStoreName.length < 2 ) continue ;
		    		if( !connection.doesTableExist( tblDesc ) ) continue ;
		    		hTables.put( tblDesc, connection.openTable( tblDesc ) ) ;
		    		tblDesc = null ;
		    	}
	    	}
		    catch( Exception e ) { throw new HBaseRdfException( "Error in finding tables that belong to store: " + name, e ) ; }
    	}
    }
    
    // Leave the getter free so the subclass can decide whether to reveal the connection or not.
    protected HBaseRdfConnection connection() 	{ return conn ; }
    
    protected String name() 					{ return name ; }  
    
    protected Map<String, HTable> tables() 		{ return hTables ; }
  
    protected long totalSize()					{ return totalSize ; }
    
    protected void addPredicateMapping( String tblName, String predURI )
    { 
    	if( predMappingTbl == null ) return ;
    	try
    	{
	    	Put put = new Put( tblName.getBytes() ) ;
			put.add( predURIColFam.getBytes(), predURI.getBytes(), Bytes.toBytes( "" ) ) ; 
			predMappingTbl.checkAndPut( tblName.getBytes(), predURIColFam.getBytes(), predURI.getBytes(), null, put ) ; 
			predMappingTbl.flushCommits() ;
    	}
    	catch( Exception e ) { throw new HBaseRdfException( "Unable to add predicate to predicate mapping table for HTable:: " + tblName, e ) ; }
    }

    protected String getPredicateMapping( String tblName )
    {
    	if( predMappingTbl == null ) return null ;
    	try
    	{
	    	Get get = new Get( tblName.getBytes() ) ;
	    	return new String( predMappingTbl.get( get ).getFamilyMap( predURIColFam.getBytes() ).keySet().iterator().next() ) ;
    	}
    	catch( Exception e ) { throw new HBaseRdfException( "Unable to retrieve predicate from predicate mapping table for HTable:: " + tblName, e ) ; }
    }
    protected void removePredicateMapping( String tblName )
    {
    	if( predMappingTbl == null ) return ;
    	try
    	{
    		Delete delete = new Delete( tblName.getBytes() ) ;
    		predMappingTbl.delete( delete ) ;
    		predMappingTbl.flushCommits() ;
    	}
    	catch( Exception e ) { throw new HBaseRdfException( "Unable to delete predicate from predicate mapping table for HTable:: " + tblName, e ) ; }
    }
    
    protected void createPrefixTbl()
    {
		try
		{
			HBaseAdmin admin = conn.getAdmin() ;
			String tblName = name() + "-predicate-mapping" ;
			
			if( !admin.tableExists( tblName ) )
			{
				HTableDescriptor tableDescriptor = new HTableDescriptor( tblName ) ;
				admin.createTable( tableDescriptor ) ;
				admin.disableTable( tblName ) ;
				
				HColumnDescriptor columnDescriptor = new HColumnDescriptor( predURIColFam ) ;
				admin.addColumn( tblName, columnDescriptor ) ;

				admin.enableTable( tblName ) ;
				predMappingTbl = new HTable( conn.getConfiguration(), tblName ) ;
			}
			else
			{
				predMappingTbl = conn.openTable( tblName ) ;
			}
		}
		catch( Exception e ) { throw new HBaseRdfException( "Unable to create predicate mapping table", e ) ; }
    }    
}
