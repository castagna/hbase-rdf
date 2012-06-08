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

import com.talis.hbase.rdf.store.TableDesc;

public class TableDescVPIndexed 
{
	private TableDesc[] tables = null ;
	
	public TableDescVPIndexed()
	{
		tables = new TableDesc[5] ;
		tables[0] = new TableDescSubjects() ;
		tables[1] = new TableDescObjects() ;
		tables[2] = new TableDescOS() ;
		tables[3] = new TableDescSPO() ;
		tables[4] = new TableDescOSP() ;
	}
	
	protected TableDesc[] getTableDesc() { return tables ; }
}
