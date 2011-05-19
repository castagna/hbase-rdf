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

public class TableDescSubjects extends TableDescSimpleCommon
{
	protected static final String SUBJECT_TBL_NAME = "-subjects" ;
    
    public static String name() { return SUBJECT_TBL_NAME ; }

    public TableDescSubjects() { this( SUBJECT_TBL_NAME ) ; }
    
    public TableDescSubjects( String tName ) { super( tName ) ; }    
}
