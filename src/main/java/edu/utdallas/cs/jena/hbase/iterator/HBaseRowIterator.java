package edu.utdallas.cs.jena.hbase.iterator;

import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import edu.utdallas.cs.jena.hbase.util.HBaseUtils;

/**
 * An iterator class over rows in a HTable.
 * @author vaibhav
 *
 */
public class HBaseRowIterator extends AbstractIterator
{
	/** The subject, predicate and object in the given TripleMatch **/
	Node subject = null, predicate = null, object = null;
	
	/** The triples that belong to a node retrieved from a HTable **/
	String[] resultTriples = null;
		
	/** A variable that keeps track of the triple being processed currently **/
	private int countOfTriples = 0;
	
	/** The row we are searching for **/
	private String row = null;
	
	/**
	 * Constructor
	 * @param rr - a row fetched from a HTable
	 * @param sm - the subject of the triple to be matched
	 * @param pm - the predicate of the triple to be matched
	 * @param om - the object of the triple to be matched
	 */
	public HBaseRowIterator( RowResult rr, Node sm, Node pm, Node om )
	{
		this.subject = sm; this.predicate = pm; this.object = om;
		this.row = Bytes.toString( rr.getRow() );
		resultTriples = Bytes.toString( rr.get( HBaseUtils.COL_FAMILY_NAME ).getValue() ).split( "&&" );
	}
	
	/**
	 * @see edu.utdallas.cs.jena.hbase.iterator.AbstractIterator#getNextObj()
	 */
	@Override
	public Object getNextObj()
	{
		Triple tr = null;
		
		//Iterate while triples still exist in the current row
		if( countOfTriples < resultTriples.length )
		{
			//Get the current triple
			String currTriple = resultTriples[countOfTriples];
			
			//Fetch the other parts of the triple, since the row itself gives us one part of the triple
			String[] remTripleParts = currTriple.split( "~~" );
			
			//If subject is concrete or triple pattern is of the form ( ANY @ANY ANY )
			//Else predicate or object processing
			if( subject.isConcrete() || ( subject.equals( Node.ANY ) && predicate.equals( Node.ANY ) && object.equals( Node.ANY ) ) )
			{
				Node pred = HBaseUtils.getNode( remTripleParts[0] ), obj = HBaseUtils.getNode( remTripleParts[1] ); 
				if( ( predicate.equals( Node.ANY ) || ( ( predicate != Node.ANY ) && predicate.equals( pred ) ) ) && ( object.equals( Node.ANY ) || ( ( object != Node.ANY ) && object.equals( obj ) ) ) )
					tr = Triple.create( HBaseUtils.getNode( row ), pred, obj );
			}
			else
				if( object.isConcrete() )
				{
					Node sub = HBaseUtils.getNode( remTripleParts[0] ), pred = HBaseUtils.getNode( remTripleParts[1] ); 
					if( ( subject.equals( Node.ANY ) || ( ( subject != Node.ANY ) && subject.equals( sub ) ) ) && ( predicate.equals( Node.ANY ) || ( ( predicate != Node.ANY ) && predicate.equals( pred ) ) ) )
						tr = Triple.create( sub, pred, HBaseUtils.getNode( row ) );
					
				}
				else if( predicate.isConcrete() )
				{
					Node sub = HBaseUtils.getNode( remTripleParts[0] ), obj = HBaseUtils.getNode( remTripleParts[1] ); 
					if( ( subject.equals( Node.ANY ) || ( ( subject != Node.ANY ) && subject.equals( sub ) ) ) && ( object.equals( Node.ANY ) || ( ( object != Node.ANY ) && object.equals( obj ) ) ) )
						tr = Triple.create( sub, HBaseUtils.getNode( row ), obj );					
				}
			//Increment the number of triples processed
			countOfTriples++;
		}	
		return tr;
	}
}
/*
* Copyright © 2010 The University of Texas at Dallas
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