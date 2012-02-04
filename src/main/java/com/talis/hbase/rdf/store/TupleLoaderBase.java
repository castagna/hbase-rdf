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

import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.TableDescLayouts;

public abstract class TupleLoaderBase extends StoreInformationHolder implements TupleLoader
{
	boolean active = false ;
	private TableDescLayouts tableDesc ;

	protected TupleLoaderBase( String storeName, HBaseRdfConnection connection, TableDescLayouts tableDesc )
	{
		this( storeName, connection ) ;
		setTableDesc( tableDesc ) ;
	}

	protected TupleLoaderBase( String storeName, HBaseRdfConnection connection ) { super( storeName, connection ) ; }

	public TableDescLayouts getTableDesc() { return tableDesc ; }
	
	public void setTableDesc( TableDescLayouts tDesc ) { this.tableDesc = tDesc ; }

	public void start()
	{
		if ( active )
			throw new HBaseRdfException( "Bulk loader already active" ) ;
		active = true ;
	}

	public void finish() { active = false ; }

	public void close() { finish(); }
}