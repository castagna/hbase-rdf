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

public class QueryRunnerSimple extends TableQueryRunnerBase implements TableQueryRunnerBasics
{
	public QueryRunnerSimple( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	@Override
	public ExtendedIterator<Triple> tableFind( Node sm, Node pm, Node om, String tblPrefix, String tblType )
	{
		ExtendedIterator<Triple> trIter = NullIterator.instance() ; 
		try
		{
			if( tblType.equalsIgnoreCase( "sub" ) )
			{
				//Get the row corresponding to the subject
				Get res = new Get( Bytes.toBytes( sm.toString() ) ) ;
				HTable table = tables().get( name() + "-" + tblPrefix + "-subjects" ) ;
				Result rr = null ; if( table != null ) rr = table.get( res ) ;
				res = null ;
	
				//Create an iterator over the triples in that row
				if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, TableDescSimpleCommon.COL_FAMILY_NAME_STR ) ;
			}
			else 
				if( tblType.equalsIgnoreCase( "obj" ) )
				{
					//Get the row corresponding to the object
					Get res = new Get( Bytes.toBytes( om.toString() ) ) ;
					HTable table = tables().get( name() + "-" + tblPrefix + "-objects" ) ;
					Result rr = null ; if( table != null ) rr = table.get( res ) ;
					res = null ;

					//Create an iterator over the triples in that row
					if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, TableDescSimpleCommon.COL_FAMILY_NAME_STR ) ;
				}
				else
					if( tblType.equalsIgnoreCase( "pred" ) )
					{
						//Get the row corresponding to the predicate
						Get res = new Get( Bytes.toBytes( pm.toString() ) ) ;
						HTable table = tables().get( name() + "-" + tblPrefix + "-predicates" ) ;
						Result rr = null ; if( table != null ) rr = table.get( res ) ;
						res = null ;
						
						//Create an iterator over the triples in that row
						if( rr != null && !rr.isEmpty() ) trIter = new HBaseRdfSingleRowIterator( rr, sm, pm, om, TableDescSimpleCommon.COL_FAMILY_NAME_STR ) ;						

					}
					else
						if( tblType.equalsIgnoreCase( "all" ) )
						{
							//Create an iterator over all rows in the subject's HTable
							Scan scanner = new Scan() ;
							HTable table = tables().get( name() + "-" + tblPrefix + "-subjects" ) ;
							if( table != null ) trIter = new HBaseRdfSingleTableIterator( table.getScanner( scanner ), sm, pm, om, TableDescSimpleCommon.COL_FAMILY_NAME_STR ) ;
						}
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in querying tables", e ) ; }
		return trIter ;
	}
}