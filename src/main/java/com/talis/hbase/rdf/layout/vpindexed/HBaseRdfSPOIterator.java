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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.iterator.AbstractIterator;
import com.talis.hbase.rdf.util.HBaseUtils;

/**
 * An iterator class over the entire subject HTable.
 */
public class HBaseRdfSPOIterator extends AbstractIterator <Triple> 
{
	/** The table scanner for the subject HTable **/
	ResultScanner scanner = null ;

	/** A single result **/
	Result result = null ;
	
	String tblSuffix = null ;
	
	public HBaseRdfSPOIterator( ResultScanner tableScanner, String tblSuffix ) 
	{
		this.scanner = tableScanner ; this.tblSuffix = tblSuffix ;
	}

	public HBaseRdfSPOIterator( Result result, String tblSuffix )
	{
		this.result = result ; this.tblSuffix = tblSuffix ;
	}
	
	@Override
	public boolean hasNext() 
	{
		try 
		{
			if( result == null && scanner != null ) 
			{
				result = scanner.next() ;
				if( result == null ) { close() ; return false ; }				
			}
			return ( result != null ) ;		
		} 
		catch( Exception e ) { throw new HBaseRdfException( "No next element found: ", e ) ; }
	}
	
	@Override
	public Triple _next() 
	{
		try 
		{
			if( result == null && scanner != null ) 
			{
				result = scanner.next() ;
				if( result == null ) { close() ; return null ; }			
			}
			Triple tr = getTriple() ; result = null ; return tr ;
		} 
		catch( Exception e ) { throw new HBaseRdfException( "Cannot retrieve table contents: ", e ) ; }
	}	
	
	private Triple getTriple()
	{
		Triple tr = null ;
		String row = new String( result.getRow() ) ;
		String[] nodesOfTriple = row.split( TableDescVPIndexedCommon.NODE_SEPARATOR ) ;
		Node n1 = HBaseUtils.getNode( nodesOfTriple[0] ), n2 = HBaseUtils.getNode( nodesOfTriple[1] ), n3 = HBaseUtils.getNode( nodesOfTriple[2] ) ; 
		if( tblSuffix.equals( TableDescSPO.SPO_TBL_NAME ) )      tr = Triple.create( n1, n2, n3 ) ;
		else if( tblSuffix.equals( TableDescOSP.OSP_TBL_NAME ) ) tr = Triple.create( n2, n3, n1 ) ;
		return tr ;
	}
}