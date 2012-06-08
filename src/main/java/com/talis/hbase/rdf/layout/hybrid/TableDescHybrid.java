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

package com.talis.hbase.rdf.layout.hybrid;

import com.talis.hbase.rdf.store.TableDesc;

public class TableDescHybrid 
{
	private TableDesc[] tables = null ;
	
	public TableDescHybrid()
	{
		tables = new TableDesc[4] ;
		tables[0] = new TableDescSubjects() ;
		tables[1] = new TableDescObjects() ;
		tables[2] = new TableDescPSubjects() ;
		tables[3] = new TableDescPObjects() ;
	}
	
	protected TableDesc[] getTableDesc() { return tables ; }
}
