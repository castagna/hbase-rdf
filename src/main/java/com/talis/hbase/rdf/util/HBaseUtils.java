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

package com.talis.hbase.rdf.util;

import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.shared.impl.JenaParameters;

/**
 * A utility class with convenience variables and methods.
 */
public class HBaseUtils 
{
	/**
	 * A method that converts a string representation into a Node
	 * @param strNode - the string representation as fetched from the HTable
	 * @return a Node representation of the given string
	 */
	public static Node getNode( String strNode ) 
	{  
		Node node = null ;
		if( strNode.startsWith( "\"", 0 ) )
		{
			String remParts = strNode.substring( strNode.lastIndexOf( "\"" ) + 1 ) ;
			strNode = strNode.substring( 1, strNode.lastIndexOf( "\"" ) ) ;
			if( remParts.equalsIgnoreCase( "" ) ) node = Node.createLiteral( strNode ) ;
			else
			{
				String[] parts = remParts.split( "\\^\\^" ) ;
				String lang = parts[0].replaceFirst( "@" , "" ) ;
				String type = null ;
				if( parts.length == 2 ) type = parts[1] ; else type = "" ;
				node = Node.createLiteral( strNode, lang, TypeMapper.getInstance().getTypeByName( type ) ) ;
			}
		}
		else if( strNode.startsWith( "_", 0 ) || ( JenaParameters.disableBNodeUIDGeneration == true && strNode.startsWith( "A" ) ) ) 
			node = Node.createAnon( new AnonId( strNode ) ) ;
		else node = Node.createURI( strNode ) ;
		
		return node ;
	}
	
	public static String getNameOfNode( Node node )
	{
		String pred = null ;
		if( node.isURI() ) pred = node.getLocalName() ;	 
		else if( node.isBlank() ) pred = node.getBlankNodeLabel() ;
		else if( node.isLiteral() ) pred = node.getLiteralValue().toString() ;
		else pred = node.toString() ;
		return pred ;
	}
}