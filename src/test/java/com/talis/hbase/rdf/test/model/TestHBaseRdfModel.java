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

package com.talis.hbase.rdf.test.model;

import com.hp.hpl.jena.rdf.model.Model;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.test.StoreCreator;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestHBaseRdfModel 
{
	public static Test suite()
	{
		TestSuite ts = new TestSuite() ;
		
		ts.addTestSuite( TestHBaseRdfSimpleModel.class ) ;
		ts.addTestSuite( TestHBaseRdfVertPartModel.class ) ;
		
		return ts ;
	}
	
	public static class TestHBaseRdfSimpleModel extends AbstractTestModelHBaseRdf
	{
		public TestHBaseRdfSimpleModel( String name ) { super( name ) ; }
		
		@Override
		public Model getModel()
		{
			Store ss = StoreCreator.getStoreSimple() ;
			return HBaseRdfFactory.connectDefaultModel( ss ) ;
		}
	}
	
	public static class TestHBaseRdfVertPartModel extends AbstractTestModelHBaseRdf
	{
		public TestHBaseRdfVertPartModel( String name ) { super( name ) ; }
		
		@Override
		public Model getModel()
		{
			Store svp = StoreCreator.getStoreVerticallyPartitioned() ;
			return HBaseRdfFactory.connectDefaultModel( svp ) ;
		}
	}
	
	public static class TestHBaseRdfIndexedModel extends AbstractTestModelHBaseRdf
	{
		public TestHBaseRdfIndexedModel( String name ) { super( name ) ; }
		
		@Override
		public Model getModel()
		{
			Store in = StoreCreator.getStoreIndexed() ;
			return HBaseRdfFactory.connectDefaultModel( in ) ;
		}
	}

	public static class TestHBaseRdfVPIndexedModel extends AbstractTestModelHBaseRdf
	{
		public TestHBaseRdfVPIndexedModel( String name ) { super( name ) ; }
		
		@Override
		public Model getModel()
		{
			Store vpin = StoreCreator.getStoreVPIndexed() ;
			return HBaseRdfFactory.connectDefaultModel( vpin ) ;
		}
	}

	public static class TestHBaseRdfHybridModel extends AbstractTestModelHBaseRdf
	{
		public TestHBaseRdfHybridModel( String name ) { super( name ) ; }
		
		@Override
		public Model getModel()
		{
			Store ss = StoreCreator.getStoreHybrid() ;
			return HBaseRdfFactory.connectDefaultModel( ss ) ;
		}
	}

	public static class TestHBaseRdfHashModel extends AbstractTestModelHBaseRdf
	{
		public TestHBaseRdfHashModel( String name ) { super( name ) ; }
		
		@Override
		public Model getModel()
		{
			Store ss = StoreCreator.getStoreHash() ;
			return HBaseRdfFactory.connectDefaultModel( ss ) ;
		}
	}
}