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

import junit.framework.Test;
import junit.framework.TestSuite;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.test.StoreCreator;

public class TestHBaseRdfGraph 
{
	public static Test suite()
	{
		TestSuite ts = new TestSuite() ;
		
		ts.addTestSuite( TestHBaseRdfSimpleGraph.class ) ;
		ts.addTestSuite( TestHBaseRdfVertPartGraph.class ) ;
		
		return ts ;
	}
	
	public static class TestHBaseRdfSimpleGraph extends AbstractTestGraphHBaseRdf
	{
		public TestHBaseRdfSimpleGraph( String name ) { super( name ) ; }
		
		@Override
		public Graph getGraph()
		{
			Store store = StoreCreator.getStoreSimple() ;
			return HBaseRdfFactory.connectDefaultGraph( store, ReificationStyle.Standard );
		}
	}
	
	public static class TestHBaseRdfVertPartGraph extends AbstractTestGraphHBaseRdf
	{
		public TestHBaseRdfVertPartGraph( String name ) { super( name ) ; }
		
		@Override
		public Graph getGraph()
		{
			Store store = StoreCreator.getStoreVerticallyPartitioned() ;
			return HBaseRdfFactory.connectDefaultGraph( store, ReificationStyle.Standard );
		}
	}
}
