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

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.talis.hbase.rdf.iterator.AbstractIterator;
import com.talis.hbase.rdf.util.HBaseUtils;

/**
 * An iterator class over rows in a HTable.
 */
public class HBaseRdfSingleRowIterator extends AbstractIterator<Triple> 
{
	/** The subject, predicate and object in the given TripleMatch **/
	Node subject = null, predicate = null, object = null ;
	
	/** An iterator over all column names for a given row **/
	Iterator<byte[]> iterColumnNames = null ;
	
	/** The row whose columns over which this iterator iterates **/
	String row = null ;
	
	/**
	 * Constructor
	 * @param rr - a row fetched from a HTable
	 * @param sm - the subject of the triple to be matched
	 * @param pm - the predicate of the triple to be matched
	 * @param om - the object of the triple to be matched
	 */
	public HBaseRdfSingleRowIterator( Result rr, Node sm, Node pm, Node om, String columnFamily )
	{
		this.subject = sm ; this.predicate = pm ; this.object = om ;
		this.row = Bytes.toString( rr.getRow() ) ;
		this.iterColumnNames = rr.getFamilyMap( columnFamily.getBytes() ).keySet().iterator() ;
	}
	
	/**
	 * @see com.talis.hbase.rdf.iterator.AbstractIterator#_next()
	 */
	@Override
	public Triple _next()
	{
		Triple tr = null ;
		
		Node trSubject = subject, trPredicate = predicate, trObject = object ;

		//Iterate while triples still exist in the current row
		while( tr == null && iterColumnNames.hasNext() )
		{			
			//Get the current triple
			String currTriple = new String( iterColumnNames.next() ) ;
			
			//Fetch the other parts of the triple, since the row itself gives us one part of the triple
			String[] remTripleParts = currTriple.split( TableDescSimpleCommon.TRIPLE_SEPARATOR ) ;
			
			//If subject is concrete or triple pattern is of the form ( ANY @ANY ANY )
			//Else predicate or object processing
			if( subject.isConcrete() || ( subject.equals( Node.ANY ) && predicate.equals( Node.ANY ) && object.equals( Node.ANY ) ) )
			{
				trSubject = HBaseUtils.getNode( row ) ; trPredicate = HBaseUtils.getNode( remTripleParts[0] ) ; trObject = HBaseUtils.getNode( remTripleParts[1] ) ; 
				if( ( predicate.equals( Node.ANY ) || ( ( predicate != Node.ANY ) && predicate.equals( trPredicate ) ) ) && ( object.equals( Node.ANY ) || ( ( object != Node.ANY ) && object.equals( trObject ) ) ) )
					tr = Triple.create( trSubject, trPredicate, trObject ) ;
			}
			else
				if( object.isConcrete() )
				{
					trSubject = HBaseUtils.getNode( remTripleParts[0] ) ; trPredicate = HBaseUtils.getNode( remTripleParts[1] ) ; trObject = HBaseUtils.getNode( row ) ;
					if( ( subject.equals( Node.ANY ) || ( ( subject != Node.ANY ) && subject.equals( trSubject ) ) ) && ( predicate.equals( Node.ANY ) || ( ( predicate != Node.ANY ) && predicate.equals( trPredicate ) ) ) )
						tr = Triple.create( trSubject, trPredicate, trObject ) ;
				}
				else if( predicate.isConcrete() )
				{
					trSubject = HBaseUtils.getNode( remTripleParts[0] ) ; trPredicate = HBaseUtils.getNode( row ) ; trObject = HBaseUtils.getNode( remTripleParts[1] ) ; 
					if( ( subject.equals( Node.ANY ) || ( ( subject != Node.ANY ) && subject.equals( trSubject ) ) ) && ( object.equals( Node.ANY ) || ( ( object != Node.ANY ) && object.equals( trObject ) ) ) )
						tr = Triple.create( trSubject, trPredicate, trObject ) ;		
				}
			
			//Clear memory
			currTriple = null ; remTripleParts = null ;
		}	
		return tr ;
	}
}