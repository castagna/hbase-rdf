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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sdb.layout2.NodeLayout2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableQueryRunnerBase;
import com.talis.hbase.rdf.layout.TableQueryRunnerBasics;
import com.talis.hbase.rdf.util.HBaseUtils;

public class QueryRunnerHash extends TableQueryRunnerBase implements TableQueryRunnerBasics
{
	public QueryRunnerHash( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	@Override
	public ExtendedIterator<Triple> tableFind( Node sm, Node pm, Node om, String tblPrefix, String tblType )
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ; 
		try
		{
			StringBuilder sbNodeTable = new StringBuilder( name() ) ; sbNodeTable.append( "-" ) ; sbNodeTable.append( tblPrefix ) ; sbNodeTable.append( TableDescNodes.name() ) ;
			HTable tblNodes = tables().get( sbNodeTable.toString() ) ; sbNodeTable = null ;
			StringBuilder sb = new StringBuilder( name() ) ; sb.append( "-" ) ; sb.append( tblPrefix ) ; 
			if( tblType.equalsIgnoreCase( "sub" ) )
			{
				//Get the row corresponding to the subject
				Get res = new Get( Bytes.toBytes( NodeLayout2.hash( sm ) ) ) ;
				if( pm.isConcrete() ) 
				{
					sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ; sb.append( TableDescPSubjects.PSUBJECT_TBL_NAME ) ;
					HTable table = tables().get( sb.toString() ) ;
					Result rr = null ; if( table != null ) rr = table.get( res ) ;
					if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, pm.toString(), TableDescHashCommon.COL_FAMILY_NAME_STR, tblNodes ) ;
				}
				else
				{
					sb.append( TableDescSubjects.SUBJECT_TBL_NAME ) ;
					HTable table = tables().get( sb.toString() ) ;
					Result rr = null ; if( table != null ) rr = table.get( res ) ;
					res = null ;
		
					//Create an iterator over the triples in that row
					if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSorOSingleRowIterator( rr, sm, pm, om, TableDescHashCommon.COL_FAMILY_NAME_STR, tblNodes ) ;
				}
				res = null ;
			}
			else 
				if( tblType.equalsIgnoreCase( "obj" ) )
				{
					Get res = new Get( Bytes.toBytes( NodeLayout2.hash( om ) ) ) ;
					if( pm.isConcrete() ) 
					{
						sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ; sb.append( TableDescPObjects.POBJECT_TBL_NAME ) ;
						HTable table = tables().get( sb.toString() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, pm.toString(), TableDescHashCommon.COL_FAMILY_NAME_STR, tblNodes ) ;
					}
					else
					{
						sb.append( TableDescObjects.OBJECT_TBL_NAME ) ;
						HTable table = tables().get( sb.toString() ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						res = null ;

						//Create an iterator over the triples in that row
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSorOSingleRowIterator( rr, sm, pm, om, TableDescHashCommon.COL_FAMILY_NAME_STR, tblNodes ) ;
					}
					res = null ;
				}
				else
					if( tblType.equalsIgnoreCase( "pred" ) )
					{
						//Create an iterator over all rows in the subject's HTable
						sb.append( "-" ) ; sb.append( HBaseUtils.getNameOfNode( pm ) ) ; sb.append( TableDescPObjects.POBJECT_TBL_NAME ) ;
						Scan scanner = new Scan() ;
						HTable table = tables().get( sb.toString() ) ;
						if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), sm, pm, om, pm.toString(), TableDescHashCommon.COL_FAMILY_NAME_STR, tblNodes ) ;
					}
					else
						if( tblType.equalsIgnoreCase( "all" ) )
						{
							//Create an iterator over all rows in the subject's HTable
							Scan scanner = new Scan() ;
							sb.append( TableDescSubjects.SUBJECT_TBL_NAME ) ;
							HTable table = tables().get( sb.toString() ) ;
							if( table != null ) trIter = new HBaseRdfSorOSingleTableIterator( table.getScanner( scanner ), sm, pm, om, TableDescHashCommon.COL_FAMILY_NAME_STR, tblNodes ) ;
						}
			sb = null ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in querying tables", e ) ; }
		return trIter ;
	}		
}
