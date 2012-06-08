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

import java.util.Iterator;

import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.FmtLayout;

public class FmtLayoutHash extends FmtLayout
{
	public FmtLayoutHash( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }
	
	@Override
	protected void formatTables()
	{		
		try
		{
			Iterator<String> iterTblNames = tables().keySet().iterator() ;
			while( iterTblNames.hasNext() )
			{
				String tblName = iterTblNames.next() ;
				removePredicateMapping( tblName ) ;
				connection().deleteTable( tblName ) ;
				tblName = null ;
			}
			tables().clear() ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Layout formatter exception", e ) ; }
	}
	
	@Override
	protected void truncateTables() { formatTables() ; }
}
