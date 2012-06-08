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

package com.talis.hbase.rdf.layout.hash;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sdb.layout2.NodeLayout2;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.iterator.AbstractIterator;
import com.talis.hbase.rdf.util.HBaseUtils;

/**
 * An iterator class over rows in a HTable.
 */
public class HBaseRdfSingleRowIterator extends AbstractIterator<Triple> 
{
	/** The subject, predicate and object in the given TripleMatch **/
	Node subject = null, predicate = null, object = null ;
	
	/** The subject, predicate and object hashes for elements of TripleMatch **/
	long subjectHash = 0L, predicateHash = 0L, objectHash = 0L ;
	
	/** An iterator over all column names for a given row **/
	Iterator<byte[]> iterColumnNames = null ;

	/** The HBase table name over which this iterator runs **/
	String pred = null ;
	
	/** The row over whose columns this iterator iterates **/
	long row = 0L, predHash = 0L ;
	
	HTable nodeTable = null ;
	
	/**
	 * Constructor
	 * @param rr - a row fetched from a HTable
	 * @param sm - the subject of the triple to be matched
	 * @param pm - the predicate of the triple to be matched
	 * @param om - the object of the triple to be matched
	 */
	public HBaseRdfSingleRowIterator( Result rr, Node sm, Node pm, Node om, String pred, String columnFamily, HTable nodeTable )
	{
		if( sm.isConcrete() ) this.subjectHash = NodeLayout2.hash( sm ) ; if( pm.isConcrete() ) this.predicateHash = NodeLayout2.hash( pm ) ; if( om.isConcrete() ) this.objectHash = NodeLayout2.hash( om ) ;
		this.subject = sm ; this.predicate = pm ; this.object = om ; this.pred = pred ; this.predHash = NodeLayout2.hash( HBaseUtils.getNode( pred ) ) ;
		this.row = Bytes.toLong( rr.getRow() ) ;
		this.iterColumnNames = rr.getFamilyMap( columnFamily.getBytes() ).keySet().iterator() ;
		this.nodeTable = nodeTable ;
	}
	
	private Node getNodeFromHash( long hash )
	{
		Node node = null ; 
		try
		{
			Get row = new Get( Bytes.toBytes( hash ) ) ;
			Result rr = nodeTable.get( row ) ;
			node = HBaseUtils.getNode( Bytes.toString( rr.getFamilyMap( TableDescNodes.COL_FAMILY_NAME_STR.getBytes() ).keySet().iterator().next() ) ) ;
		}
		catch( Exception e ) { throw new HBaseRdfException( "Error in converting hash to node", e ) ; }
		return node ;
	}
	
	/**
	 * @see com.talis.hbase.rdf.iterator.AbstractIterator#_next()
	 */
	@Override
	public Triple _next()
	{
		Triple tr = null ;
		
		//Iterate while triples still exist in the current row
		while( tr == null && iterColumnNames.hasNext() )
		{
			Node trSubject = subject, trPredicate = predicate, trObject = object ; 
			long trSubjectHash = subjectHash, trPredicateHash = predicateHash, trObjectHash = objectHash ; 
			
			//Get the current object
			long nodeHash = Bytes.toLong( iterColumnNames.next() ) ;
			
			//If subject is concrete or triple pattern is of the form ( ANY @ANY ANY )
			//Else predicate or object processing
			if( subject.isConcrete() || ( subject.equals( Node.ANY ) && predicate.equals( Node.ANY ) && object.equals( Node.ANY ) ) )
			{
				trSubjectHash = row ; trPredicateHash = predHash ; trObjectHash = nodeHash ; 
				if( ( predicate.equals( Node.ANY ) || ( ( predicate != Node.ANY ) && predicateHash == trPredicateHash ) ) && 
					( object.equals( Node.ANY )    || ( ( object != Node.ANY )    && objectHash == trObjectHash ) ) )
				{
					trSubject = getNodeFromHash( trSubjectHash ) ; trPredicate = getNodeFromHash( trPredicateHash ) ; trObject = getNodeFromHash( trObjectHash ) ;
					tr = Triple.create( trSubject, trPredicate, trObject ) ;
				}
			}
			else
				if( object.isConcrete() )
				{
					trSubjectHash = nodeHash ; trPredicateHash = predHash ; trObjectHash = row ; 
					if( ( subject.equals( Node.ANY )   || ( ( subject != Node.ANY )   && subjectHash == trSubjectHash ) ) && 
						( predicate.equals( Node.ANY ) || ( ( predicate != Node.ANY ) && predicateHash == trPredicateHash ) ) )
					{
						trSubject = getNodeFromHash( trSubjectHash ) ; trPredicate = getNodeFromHash( trPredicateHash ) ; trObject = getNodeFromHash( trObjectHash ) ;
						tr = Triple.create( trSubject, trPredicate, trObject ) ;
					}
				}
				else
					if( predicate.isConcrete() )
					{
						trSubjectHash = row ; trPredicateHash = predHash ; trObjectHash = nodeHash ;
						if( ( subject.equals( Node.ANY )   || ( ( subject != Node.ANY )   && subjectHash == trSubjectHash ) ) && 
							( object.equals( Node.ANY )    || ( ( object != Node.ANY )    && objectHash == trObjectHash ) ) )
						{
							trSubject = getNodeFromHash( trSubjectHash ) ; trPredicate = getNodeFromHash( trPredicateHash ) ; trObject = getNodeFromHash( trObjectHash ) ;
							tr = Triple.create( trSubject, trPredicate, trObject ) ;
						}
					}
		}	
		return tr ;
	}
}
