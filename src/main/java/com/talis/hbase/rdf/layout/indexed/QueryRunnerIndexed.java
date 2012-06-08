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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableQueryRunnerBase;
import com.talis.hbase.rdf.layout.TableQueryRunnerBasics;

public class QueryRunnerIndexed extends TableQueryRunnerBase implements TableQueryRunnerBasics
{
	public QueryRunnerIndexed( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	@Override
	public ExtendedIterator<Triple> tableFind( Node sm, Node pm, Node om, String tblPrefix, String tblType )
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ; 
		try
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ; 
			Scan scanner = null ; 
			if( tblType.equals( "sub" ) )
			{
				String smString = sm.toString() ;
				if( pm.equals( Node.ANY ) && om.equals( Node.ANY ) || pm.isConcrete() && om.equals( Node.ANY ) || pm.isConcrete() && om.isConcrete() )
				{
					sb.append( TableDescSPO.SPO_TBL_NAME ) ; HTable table = tables().get( sb.toString() ) ; sb = null ;
					if( pm.equals( Node.ANY ) && om.equals( Node.ANY ) ) 
					{ 
						scanner = new Scan( smString.getBytes() ) ; scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( smString.getBytes() ) ) ) ; 
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescSPO.SPO_TBL_NAME ) ;
					}
					else if( pm.isConcrete() && om.equals( Node.ANY ) )
					{
						scanner = new Scan( smString.getBytes() ) ;
						sb = new StringBuilder( smString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( pm.toString() ) ;
						scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( sb.toString().getBytes() ) ) ) ;						
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescSPO.SPO_TBL_NAME ) ;
					}
					else if( pm.isConcrete() && om.isConcrete() )
					{
						sb = new StringBuilder( smString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( pm.toString() ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( om.toString() ) ;
						Get res = new Get( sb.toString().getBytes() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleTableIterator( rr, TableDescSPO.SPO_TBL_NAME ) ;
					}
				}
				else if( om.isConcrete() && pm.equals( Node.ANY ) )
				{
					sb.append( TableDescSOP.SOP_TBL_NAME ) ; HTable table = tables().get( sb.toString() ) ; sb = null ;
					sb = new StringBuilder( smString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( om.toString() ) ;
					scanner = new Scan( smString.getBytes() ) ; scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( sb.toString().getBytes() ) ) ) ;
					if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescSOP.SOP_TBL_NAME ) ;
				}
			}
			else if( tblType.equals( "obj" ) )
			{
				String omString = om.toString() ;
				if( sm.equals( Node.ANY ) && pm.equals( Node.ANY ) || pm.isConcrete() && sm.equals( Node.ANY ) || pm.isConcrete() && sm.isConcrete() )
				{
					sb.append( TableDescOPS.OPS_TBL_NAME ) ; HTable table = tables().get( sb.toString() ) ; sb = null ;
					if( sm.equals( Node.ANY ) && pm.equals( Node.ANY ) ) 
					{
						scanner = new Scan( omString.getBytes() ) ; scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( omString.getBytes() ) ) ) ;
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescOPS.OPS_TBL_NAME ) ;
					}
					else if( pm.isConcrete() && sm.equals( Node.ANY ) )
					{
						scanner = new Scan( omString.getBytes() ) ;
						sb = new StringBuilder( omString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( pm.toString() ) ;
						scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( sb.toString().getBytes() ) ) ) ;
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescOPS.OPS_TBL_NAME ) ;
					}
					else if( pm.isConcrete() && sm.isConcrete() )
					{
						sb = new StringBuilder( omString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( pm.toString() ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( sm.toString() ) ;
						Get res = new Get( sb.toString().getBytes() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleTableIterator( rr, TableDescOPS.OPS_TBL_NAME ) ;
					}
				}
				else if( sm.isConcrete() && pm.equals( Node.ANY ) )
				{
					sb.append( TableDescOSP.OSP_TBL_NAME ) ; HTable table = tables().get( sb.toString() ) ; sb = null ;
					sb = new StringBuilder( omString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( sm.toString() ) ;
					scanner = new Scan( omString.getBytes() ) ; scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( sb.toString().getBytes() ) ) ) ;
					if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescOSP.OSP_TBL_NAME ) ;							
				}				
			}
			else if( tblType.equals( "pred" ) )
			{
				String pmString = pm.toString() ;
				if( ( sm.equals( Node.ANY ) && om.equals( Node.ANY ) ) || ( sm.isConcrete() && om.equals( Node.ANY ) ) || ( sm.isConcrete() && om.isConcrete() ) )
				{
					sb.append( TableDescPSO.PSO_TBL_NAME ) ; HTable table = tables().get( sb.toString() ) ; sb = null ;
					if( sm.equals( Node.ANY ) && om.equals( Node.ANY ) ) 
					{ 
						scanner = new Scan( pmString.getBytes() ) ; scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( pmString.getBytes() ) ) ) ; 
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescPSO.PSO_TBL_NAME ) ;
					}
					else if( sm.isConcrete() && om.equals( Node.ANY ) )
					{
						scanner = new Scan( pmString.getBytes() ) ;
						sb = new StringBuilder( pmString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( sm.toString() ) ;
						scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( sb.toString().getBytes() ) ) ) ;						
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescPSO.PSO_TBL_NAME ) ;
					}
					else if( sm.isConcrete() && om.isConcrete() )
					{
						sb = new StringBuilder( pmString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( sm.toString() ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( om.toString() ) ;
						Get res = new Get( sb.toString().getBytes() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleTableIterator( rr, TableDescPSO.PSO_TBL_NAME ) ;
					}
				}
				else if( om.isConcrete() && sm.equals( Node.ANY ) )
				{
					sb.append( TableDescPOS.POS_TBL_NAME ) ; HTable table = tables().get( sb.toString() ) ; sb = null ;
					sb = new StringBuilder( pmString ) ; sb.append( TableDescIndexedCommon.TRIPLE_SEPARATOR ) ; sb.append( om.toString() ) ;
					scanner = new Scan( pmString.getBytes() ) ; scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( sb.toString().getBytes() ) ) ) ;
					if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescPOS.POS_TBL_NAME ) ;								
				}				
			}
			else if( tblType.equals( "all" ) )
			{
				sb.append( TableDescSPO.SPO_TBL_NAME ) ;
				HTable table = tables().get( sb.toString() ) ; sb = null ;
				scanner = new Scan() ;
				if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), TableDescSPO.SPO_TBL_NAME ) ;							
			}
			scanner = null ; sb = null ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in querying tables", e ) ; }
		return trIter ;
	}	
}