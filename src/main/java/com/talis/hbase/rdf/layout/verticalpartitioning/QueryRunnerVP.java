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

package com.talis.hbase.rdf.layout.verticalpartitioning;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableQueryRunnerBase;
import com.talis.hbase.rdf.layout.TableQueryRunnerBasics;
import com.talis.hbase.rdf.util.HBaseUtils;

public class QueryRunnerVP extends TableQueryRunnerBase implements TableQueryRunnerBasics
{
	public QueryRunnerVP( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	@Override
	public ExtendedIterator<Triple> tableFind( Node sm, Node pm, Node om, String tblPrefix, String tblType )
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ; 
		try
		{
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ; sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ;
			if( tblType.equalsIgnoreCase( "sub" ) )
			{
				//Get the row corresponding to the subject
				Get res = new Get( Bytes.toBytes( sm.toString() ) ) ;
				if( pm.isConcrete() ) 
				{
					sb.append( TableDescSubjects.SUBJECT_TBL_NAME ) ;
					HTable table = tables().get( sb.toString() ) ;
					Result rr = null ; if( table != null ) rr = table.get( res ) ;
					if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, pm.toString(), TableDescVPCommon.COL_FAMILY_NAME_STR ) ;
				}
				else
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
							( ( HBaseRdfAllTablesIterator )trIter ).addIter( new HBaseRdfSingleRowIterator( rr, sm, pm, om, getPredicateMapping( tblName ), TableDescVPCommon.COL_FAMILY_NAME_STR ) ) ;
						tblName = null ; mapPrefix = null ;
					}
					( ( HBaseRdfAllTablesIterator )trIter ).closeIter() ;
				}
				res = null ;
			}
			else 
				if( tblType.equalsIgnoreCase( "obj" ) )
				{
					Get res = new Get( Bytes.toBytes( om.toString() ) ) ;
					if( pm.isConcrete() ) 
					{
						sb.append( TableDescObjects.OBJECT_TBL_NAME ) ;
						HTable table = tables().get( sb.toString() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, pm.toString(), TableDescVPCommon.COL_FAMILY_NAME_STR ) ;
					}
					else
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
								( ( HBaseRdfAllTablesIterator )trIter ).addIter( new HBaseRdfSingleRowIterator( rr, sm, pm, om, getPredicateMapping( tblName ), TableDescVPCommon.COL_FAMILY_NAME_STR ) ) ;
							tblName = null ; mapPrefix = null ;
						}
						( ( HBaseRdfAllTablesIterator )trIter ).closeIter() ;
					}
					res = null ;
				}
				else
					if( tblType.equalsIgnoreCase( "pred" ) )
					{
						//Create an iterator over all rows in the subject's HTable
						Scan scanner = new Scan() ;
						HTable table = tables().get( name() + "-" + tblPrefix + "-" + HBaseUtils.getNameOfNode( pm ) + "-subjects" ) ;
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), sm, pm, om, pm.toString(), TableDescVPCommon.COL_FAMILY_NAME_STR ) ;
					}
					else
						if( tblType.equalsIgnoreCase( "all" ) )
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
								Scan scanner = new Scan() ;
								if( table != null ) 
									( ( HBaseRdfAllTablesIterator )trIter ).addIter( new HBaseRdfSingleTableIterator( table.getScanner( scanner ), sm, pm, om, getPredicateMapping( tblName ), TableDescVPCommon.COL_FAMILY_NAME_STR ) ) ;
								tblName = null ; mapPrefix = null ;
							}
							( ( HBaseRdfAllTablesIterator )trIter ).closeIter() ;
						}
			sb = null ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in querying tables", e ) ; }
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