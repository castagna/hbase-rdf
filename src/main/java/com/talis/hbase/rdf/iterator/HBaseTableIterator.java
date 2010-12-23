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

package com.talis.hbase.rdf.iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.JenaException;

/**
 * An iterator class over the entire subject HTable.
 */
public class HBaseTableIterator extends AbstractIterator <Triple> {

	/** The table scanner for the subject HTable **/
	ResultScanner scanner = null;
	
	/** An iterator over a single row in the scanner **/
	HBaseSingleRowIterator rowIterator = null;
	
	/** The subject, predicate and object nodes to be matched **/
	Node subject = null, predicate = null, object = null;
	
	/**
	 * 
	 * @param tableScanner - a scanner over the subject HTable
	 * @param sm - the subject of the triple to be matched
	 * @param pm - the predicate of the triple to be matched
	 * @param om - the object of the triple to be matched
	 */
	public HBaseTableIterator( ResultScanner tableScanner, Node sm, Node pm, Node om ) 
	{
		this.scanner = tableScanner; this.subject = sm; this.predicate = pm; this.object = om;
	}
	
	@Override
	public boolean hasNext() 
	{
		try 
		{
			if( rowIterator == null || !rowIterator.hasNext() ) 
			{
				Result rr = scanner.next();
				if( rr == null ) { rowIterator = null; return false; }				
				rowIterator = new HBaseSingleRowIterator( rr, subject, predicate, object );
			}
			return rowIterator.hasNext();		
		} 
		catch( Exception e ) { throw new JenaException( "No next element found: ", e ); }
	}
	
	@Override
	public Triple _next() 
	{
		try 
		{
			if( rowIterator == null || !rowIterator.hasNext() ) 
			{
				Result rr = scanner.next();
				if( rr == null ) { rowIterator = null; return null; }			
				rowIterator = new HBaseSingleRowIterator( rr, subject, predicate, object );
			}
			return (Triple)rowIterator.next();
		} 
		catch( Exception e ) { throw new JenaException( "Cannot retrieve table contents: ", e ); }
	}
	
}

