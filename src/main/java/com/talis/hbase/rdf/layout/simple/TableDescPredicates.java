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

public class TableDescPredicates extends TableDescSimpleCommon
{
	protected static final String PREDICATE_TBL_NAME = "-predicates" ;
    
    public static String name() { return PREDICATE_TBL_NAME ; }

    public TableDescPredicates() { this( PREDICATE_TBL_NAME ) ; }
    
    public TableDescPredicates( String tName ) { super( tName ) ; }    
}
