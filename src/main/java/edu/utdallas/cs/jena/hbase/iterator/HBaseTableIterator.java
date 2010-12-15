package edu.utdallas.cs.jena.hbase.iterator;

import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.JenaException;

/**
 * An iterator class over the entire subject HTable.
 * @author cloud
 *
 */
public class HBaseTableIterator extends AbstractIterator 
{
	/** The table scanner for the subject HTable **/
	Scanner scanner = null;
	
	/** An iterator over rows in the scanner **/
	HBaseRowIterator rowIterator = null;
	
	/** The subject, predicate and object nodes to be matched **/
	Node subject = null, predicate = null, object = null;
	
	/**
	 * 
	 * @param tableScanner - a scanner over the subject HTable
	 * @param sm - the subject of the triple to be matched
	 * @param pm - the predicate of the triple to be matched
	 * @param om - the object of the triple to be matched
	 */
	public HBaseTableIterator( Scanner tableScanner, Node sm, Node pm, Node om )
	{
		this.scanner = tableScanner; this.subject = sm; this.predicate = pm; this.object = om;
	}
	
	/**
	 * @see edu.utdallas.cs.jena.hbase.iterator.AbstractIterator#hasNext()
	 */
	@Override
	public boolean hasNext()
	{
		try
		{
			if( rowIterator == null || !rowIterator.hasNext() )
			{
				RowResult rr = scanner.next();
				if( rr == null ) { rowIterator = null; return false; }				
				rowIterator = new HBaseRowIterator( rr, subject, predicate, object );
			}
			return rowIterator.hasNext();		
		}
		catch( Exception e ) { throw new JenaException( "No next element found: ", e ); }
	}
	
	/**
	 * @see edu.utdallas.cs.jena.hbase.iterator.AbstractIterator#getNextObj()
	 */
	@Override
	public Object getNextObj()
	{
		try
		{
			if( rowIterator == null || !rowIterator.hasNext() )
			{
				RowResult rr = scanner.next();
				if( rr == null ) { rowIterator = null; return null; }			
				rowIterator = new HBaseRowIterator( rr, subject, predicate, object );
			}
			return (Triple)rowIterator.next();
		}
		catch( Exception e ) { throw new JenaException( "Cannot retrieve table contents: ", e ); }
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