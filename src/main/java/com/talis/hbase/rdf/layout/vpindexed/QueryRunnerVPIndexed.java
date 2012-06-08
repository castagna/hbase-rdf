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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableQueryRunnerBase;
import com.talis.hbase.rdf.layout.TableQueryRunnerBasics;
import com.talis.hbase.rdf.layout.verticalpartitioning.HBaseRdfAllTablesIterator;
import com.talis.hbase.rdf.layout.verticalpartitioning.HBaseRdfSingleRowIterator;
import com.talis.hbase.rdf.util.HBaseUtils;

public class QueryRunnerVPIndexed extends TableQueryRunnerBase implements TableQueryRunnerBasics
{
	public QueryRunnerVPIndexed( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	@Override
	public ExtendedIterator<Triple> tableFind( Node sm, Node pm, Node om, String tblPrefix, String tblType )
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ; 
		try
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ;
			if( tblType.equalsIgnoreCase( "sub" ) )
			{
				//Get the row corresponding to the subject
				Get res = new Get( Bytes.toBytes( sm.toString() ) ) ;
				if( pm.isConcrete() && om.equals( Node.ANY ) ) 
				{
					sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ; sb.append( TableDescSubjects.SUBJECT_TBL_NAME ) ;
					HTable table = tables().get( sb.toString() ) ;
					Result rr = null ; if( table != null ) rr = table.get( res ) ;
					if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, pm.toString(), TableDescVPIndexedCommon.COL_FAMILY_NAME_STR ) ;
				}
				else if( ( pm.equals( Node.ANY ) && om.isConcrete() ) || ( pm.equals( Node.ANY ) && om.equals( Node.ANY ) ) )
				{
					if( ( ( pm.equals( Node.ANY ) && om.isConcrete() ) && checkOSExists( sm, om, tblPrefix ) ) || ( pm.equals( Node.ANY ) && om.equals( Node.ANY ) )  )
					{
						trIter = new HBaseRdfAllTablesIterator() ;
						//Iterate over all tables to find all triples for the subject
						Iterator<String> iterTblNames = tables().keySet().iterator() ;
						while( iterTblNames.hasNext() )
						{
							String tblName = iterTblNames.next() ;
							String mapPrefix = processTblName( tblName, tblPrefix, "subjects", "objects" ) ; 
							if( mapPrefix == null ) continue ;
							HTable table = tables().get( tblName ) ;
							Result rr = null ; if( table != null ) rr = table.get( res ) ;
							if( rr != null && !rr.isEmpty() ) 
								( ( HBaseRdfAllTablesIterator )trIter ).addIter( new HBaseRdfSingleRowIterator( rr, sm, pm, om, getPredicateMapping( tblName ), TableDescVPIndexedCommon.COL_FAMILY_NAME_STR ) ) ;
							tblName = null ; mapPrefix = null ;
						}
						( ( HBaseRdfAllTablesIterator )trIter ).closeIter() ;
 					}
				}
				else if( pm.isConcrete() && om.isConcrete() )
				{
					trIter = checkSPOExists( sm, pm, om, tblPrefix, TableDescSPO.SPO_TBL_NAME ) ;
				}
				res = null ;
			}
			else 
				if( tblType.equalsIgnoreCase( "obj" ) )
				{
					Get res = new Get( Bytes.toBytes( om.toString() ) ) ;
					if( pm.isConcrete() && sm.equals( Node.ANY )) 
					{
						sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ; sb.append( TableDescObjects.OBJECT_TBL_NAME ) ;
						HTable table = tables().get( sb.toString() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, pm.toString(), TableDescVPIndexedCommon.COL_FAMILY_NAME_STR ) ;
					}
					else if( ( pm.equals( Node.ANY ) && sm.isConcrete() ) || ( pm.equals( Node.ANY ) && sm.equals( Node.ANY ) ) )
					{
						if( ( ( pm.equals( Node.ANY ) && sm.isConcrete() ) && checkOSExists( sm, om, tblPrefix ) ) || ( pm.equals( Node.ANY ) && sm.equals( Node.ANY ) ) )
						{
							trIter = new HBaseRdfAllTablesIterator() ;
							//Iterate over all tables to find all triples for the subject
							Iterator<String> iterTblNames = tables().keySet().iterator() ;
							while( iterTblNames.hasNext() )
							{
								String tblName = iterTblNames.next() ;
								String mapPrefix = processTblName( tblName, tblPrefix, "objects", "subjects" ) ; 
								if( mapPrefix == null ) continue ;
								HTable table = tables().get( tblName ) ;
								Result rr = null ; if( table != null ) rr = table.get( res ) ;
								if( rr != null && !rr.isEmpty() ) 
									( ( HBaseRdfAllTablesIterator )trIter ).addIter( new HBaseRdfSingleRowIterator( rr, sm, pm, om, getPredicateMapping( tblName ), TableDescVPIndexedCommon.COL_FAMILY_NAME_STR ) ) ;
								tblName = null ; mapPrefix = null ;
							}
							( ( HBaseRdfAllTablesIterator )trIter ).closeIter() ;
						}
					}
					else if( pm.isConcrete() && sm.isConcrete() )
					{
						trIter = checkSPOExists( sm, pm, om, tblPrefix, TableDescOSP.OSP_TBL_NAME ) ;
					}
					res = null ;
				}
				else
					if( tblType.equalsIgnoreCase( "pred" ) )
					{
						//Create an iterator over all rows in the subject's HTable
						Scan scanner = new Scan() ;
						sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ; sb.append( TableDescSubjects.SUBJECT_TBL_NAME ) ;
						HTable table = tables().get( sb.toString() ) ; sb = null ;
 						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), sm, pm, om, pm.toString(), TableDescVPIndexedCommon.COL_FAMILY_NAME_STR ) ;
					}
					else
						if( tblType.equalsIgnoreCase( "all" ) )
						{
							Scan scanner = new Scan() ;
							sb.append( TableDescSPO.SPO_TBL_NAME ) ;
							HTable table = tables().get( sb.toString() ) ; sb = null ;
							if( table != null ) trIter = new HBaseRdfSPOIterator( table.getScanner( scanner ), TableDescSPO.SPO_TBL_NAME ) ;
						}
			sb = null ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in querying tables", e ) ; }
		return trIter ;
	}	

	@SuppressWarnings("unused")
	private ExtendedIterator<Triple> nodeIterator( Node no, String tblPrefix, String tblSuffix ) throws IOException
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ;

		StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ;
		if( tblSuffix.equals( TableDescSPO.SPO_TBL_NAME ) ) sb.append( TableDescSPO.SPO_TBL_NAME ) ;
		else sb.append( TableDescOSP.OSP_TBL_NAME ) ;
		HTable table = tables().get( sb.toString() ) ;
		Scan scanner = new Scan( no.toString().getBytes() ) ;
		scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( no.toString().getBytes() ) ) ) ;
		if( table != null ) trIter = new HBaseRdfSPOIterator( table.getScanner( scanner ), tblSuffix ) ;
		
		return trIter ;
	}
	
	private boolean checkOSExists( Node sm, Node om, String tblPrefix ) throws IOException
	{
		boolean OSExists = false ;

		StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ; sb.append( TableDescOS.OS_TBL_NAME ) ;
		HTable table = tables().get( sb.toString() ) ; sb = null ;
		sb = new StringBuilder( om.toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; sb.append( sm.toString() ) ;
		Get getOSExists = new Get( sb.toString().getBytes() ) ;
		Result rr = null ; if( table != null ) rr = table.get( getOSExists ) ;
		if( rr != null && !rr.isEmpty() ) OSExists = true ;

		return OSExists ;
	}
	
	private ExtendedIterator<Triple> checkSPOExists( Node sm, Node pm, Node om, String tblPrefix, String tblSuffix ) throws IOException
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ;

		StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ; sb.append( TableDescSPO.SPO_TBL_NAME ) ;
		HTable table = tables().get( sb.toString() ) ; sb = null ;
		sb = new StringBuilder( sm.toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; sb.append( pm.toString() ) ; sb.append( TableDescVPIndexedCommon.NODE_SEPARATOR ) ; sb.append( om.toString() ) ;
		Get getSPOExists = new Get( sb.toString().getBytes() ) ;
		Result rr = null ; if( table != null ) rr = table.get( getSPOExists ) ;
		if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSPOIterator( rr, tblSuffix ) ;

		return trIter ;
	}

	private String processTblName( String tblName, String tblPrefix, String tblSuffix, String otherSuffix )
	{
		String mapPrefix = null ;
		String[] splitStoreName = tblName.split( name() ) ; if( splitStoreName.length < 2 ) return mapPrefix ;
		if( splitStoreName.length == 2 )
		{
			String[] splitTblPrefix = splitStoreName[1].split( tblPrefix ) ; if( splitTblPrefix.length < 2 ) return mapPrefix ;
			if( !splitTblPrefix[0].equals( "-" ) ) return mapPrefix ;
			String[] splitSO = null ;
			if( splitTblPrefix.length == 2 ) 
			{
				splitSO = splitTblPrefix[1].split( tblSuffix ) ;
				if( splitSO.length > 1 || splitSO[0].contains( otherSuffix ) ) return mapPrefix ;
				mapPrefix = splitSO[0].substring( 1, splitSO[0].length() - 1 ) ;
			}			
			else if( splitTblPrefix.length == 3 ) 
			{
				splitSO = splitTblPrefix[2].split( tblSuffix ) ; 
				if( splitSO.length > 1 || splitSO[0].contains( otherSuffix ) ) return mapPrefix ;
				mapPrefix = tblPrefix ;
			}
		}
		else if( splitStoreName.length == 3 )
		{
			if( !splitStoreName[0].equals( "" ) ) return mapPrefix ;
			if( splitStoreName[1].equals( "-" ) )
			{
				String[] splitSO = splitStoreName[2].split( tblSuffix ) ;
				if( splitSO.length > 1 || splitSO[0].contains( otherSuffix ) ) return mapPrefix ;
				mapPrefix = splitSO[0].substring( 1, splitSO[0].length() - 1 ) ;				
			}
			else
			{
				String[] splitSO = splitStoreName[2].split( tblSuffix ) ;
				if( splitSO.length > 1 || splitSO[0].contains( otherSuffix ) ) return mapPrefix ;
				mapPrefix = name() ;				
			}
		}
		else if( splitStoreName.length == 4 )
		{
			if( !splitStoreName[0].equals( "" ) ) return mapPrefix ;
			String[] splitSO = splitStoreName[3].split( tblSuffix ) ;
			if( splitSO.length > 1 || splitSO[0].contains( otherSuffix ) ) return mapPrefix ;
			mapPrefix = splitSO[0].substring( 1, splitSO[0].length() - 1 ) ;
		}
		return mapPrefix ;
	}
}