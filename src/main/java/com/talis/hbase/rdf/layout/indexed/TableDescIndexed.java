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

import com.talis.hbase.rdf.store.TableDesc;

public class TableDescIndexed 
{
	private TableDesc[] tables = null ;
	
	public TableDescIndexed()
	{
		tables = new TableDesc[6] ;
		tables[0] = new TableDescSPO() ;
		tables[1] = new TableDescSOP() ;
		tables[2] = new TableDescPSO() ;
		tables[3] = new TableDescPOS() ;
		tables[4] = new TableDescOSP() ;
		tables[5] = new TableDescOPS() ;
	}
	
	protected TableDesc[] getTableDesc() { return tables ; }

}
