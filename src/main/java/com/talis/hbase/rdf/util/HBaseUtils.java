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

package com.talis.hbase.rdf.util;

import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.test.NodeCreateUtils;

/**
 * A utility class with convenience variables and methods.
 *
 */
public class HBaseUtils 
{
	/** The suffix for the subject HTable **/
	public static final String SUBJECT_TBL_NAME = "subjects";

	/** The suffix for the predicate HTable **/
	public static final String PREDICATE_TBL_NAME = "predicates";
	
	/** The suffix for the object HTable **/
	public static final String OBJECT_TBL_NAME = "objects";
	
	/** The column family value in each HTable **/
	public static final String COL_FAMILY_NAME_STR = "triples";
	
	/** The column family name as a byte array **/
	public static final byte[] COL_FAMILY_NAME_BYTES = Bytes.toBytes( COL_FAMILY_NAME_STR );

	/** The qualifier for the triples column in each HTable **/
	public static final String COL_QUALIFIER_NAME_STR = "triples";

	/** The qualifier as a byte array **/
	public static final byte[] COL_QUALIFIER_NAME_BYTES = Bytes.toBytes( COL_QUALIFIER_NAME_STR );

	/** Separator characters between triples in a cell **/
	public static final String CELL_VALUE_SEPARATOR = "&&";
	
	/** Separator characters between the parts of a triple being stored **/
	public static final String TRIPLE_SEPARATOR = "~~";
		
	/**
	 * A method that converts a string representation into a Node
	 * @param strNode - the string representation as fetched from the HTable
	 * @return a Node representation of the given string
	 */
	public static Node getNode( String strNode ) { return NodeCreateUtils.create( strNode ); }
}