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
		ts.addTestSuite( TestHBaseRdfIndexedGraph.class ) ;
		ts.addTestSuite( TestHBaseRdfVPIndexedGraph.class ) ;
		ts.addTestSuite( TestHBaseRdfHybridGraph.class ) ;
		ts.addTestSuite( TestHBaseRdfHashGraph.class ) ;
		
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
	
	public static class TestHBaseRdfIndexedGraph extends AbstractTestGraphHBaseRdf
	{
		public TestHBaseRdfIndexedGraph( String name ) { super( name ) ; }
		
		@Override
		public Graph getGraph()
		{
			Store store = StoreCreator.getStoreIndexed() ;
			return HBaseRdfFactory.connectDefaultGraph( store, ReificationStyle.Standard );
		}
	}

	public static class TestHBaseRdfVPIndexedGraph extends AbstractTestGraphHBaseRdf
	{
		public TestHBaseRdfVPIndexedGraph( String name ) { super( name ) ; }
		
		@Override
		public Graph getGraph()
		{
			Store store = StoreCreator.getStoreVPIndexed() ;
			return HBaseRdfFactory.connectDefaultGraph( store, ReificationStyle.Standard );
		}
	}

	public static class TestHBaseRdfHybridGraph extends AbstractTestGraphHBaseRdf
	{
		public TestHBaseRdfHybridGraph( String name ) { super( name ) ; }
		
		@Override
		public Graph getGraph()
		{
			Store store = StoreCreator.getStoreHybrid() ;
			return HBaseRdfFactory.connectDefaultGraph( store, ReificationStyle.Standard );
		}
	}

	public static class TestHBaseRdfHashGraph extends AbstractTestGraphHBaseRdf
	{
		public TestHBaseRdfHashGraph( String name ) { super( name ) ; }
		
		@Override
		public Graph getGraph()
		{
			Store store = StoreCreator.getStoreHash() ;
			return HBaseRdfFactory.connectDefaultGraph( store, ReificationStyle.Standard );
		}
	}
}