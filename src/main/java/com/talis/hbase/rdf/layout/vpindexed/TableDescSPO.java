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

public class TableDescSPO extends TableDescVPIndexedCommon
{
	protected static final String SPO_TBL_NAME = "-SPO" ;
    
    public static String name() { return SPO_TBL_NAME ; }

    public TableDescSPO() { this( SPO_TBL_NAME ) ; }
    
    public TableDescSPO( String tName ) { super( tName ) ; }    
}
