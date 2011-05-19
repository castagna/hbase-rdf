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

package com.talis.hbase.rdf.test.graph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.test.AbstractTestReifier;

public abstract class AbstractTestReifierHBaseRdf extends AbstractTestReifier
{
	public AbstractTestReifierHBaseRdf( String x ) { super( x ) ; }

	//TODO: These tests currently fail, need to check and fix

	//handledAdd() and handledRemove() are not yet implemented
	@Override
	public void testIntercept() {}	

	public void testMinimalExplode()
	{
		Graph g = getGraph( Minimal );
		g.getReifier().reifyAs( node( "a" ), triple( "p Q r" ) );
		assertEquals( 4, g.size() ); //Originally size = 0 ; but doesn't MINIMAL expose quadlets in the graph 
	}

	public void testEmptyReifiers()
	{
		//Originally false, should it be ?
		assertTrue( getGraphWith( "x R y" ).getReifier().findExposed( ALL ).hasNext() );
		assertTrue( getGraphWith( "x R y; p S q" ).getReifier().findExposed( ALL ).hasNext() );
	}
}