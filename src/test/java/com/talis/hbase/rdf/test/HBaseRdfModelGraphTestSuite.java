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

package com.talis.hbase.rdf.test;

import junit.framework.TestSuite;
import org.junit.runners.AllTests;

import org.junit.runner.RunWith;

import com.talis.hbase.rdf.test.graph.TestHBaseRdfGraph;
import com.talis.hbase.rdf.test.graph.TestHBaseRdfReifier;
import com.talis.hbase.rdf.test.model.TestHBaseRdfModel;

@RunWith(AllTests.class)
public class HBaseRdfModelGraphTestSuite extends TestSuite
{
	static boolean includeSimple = true ;
	static boolean includeVertPart = true ;
	static boolean includeIndexed = true ;
	static boolean includeVPIndexed = true ;
	static boolean includeHybrid = true ;
	static boolean includeHash = true ;
	
    public static junit.framework.Test suite() 
    {
    	TestSuite ts = new TestSuite();
    	
    	if( includeSimple )
    	{
	       	ts.addTestSuite( TestHBaseRdfModel.TestHBaseRdfSimpleModel.class ) ;
	       	ts.addTestSuite( TestHBaseRdfGraph.TestHBaseRdfSimpleGraph.class ) ;
	       	ts.addTestSuite( TestHBaseRdfReifier.TestHBaseRdfSimpleReifier.class ) ;
    	}
    	
    	if( includeVertPart )
    	{
    		ts.addTestSuite( TestHBaseRdfModel.TestHBaseRdfVertPartModel.class ) ;
    		ts.addTestSuite( TestHBaseRdfGraph.TestHBaseRdfVertPartGraph.class ) ;
    		ts.addTestSuite( TestHBaseRdfReifier.TestHBaseRdfVertPartReifier.class ) ;
    	}

    	if( includeIndexed )
    	{
	       	ts.addTestSuite( TestHBaseRdfModel.TestHBaseRdfIndexedModel.class ) ;
	       	ts.addTestSuite( TestHBaseRdfGraph.TestHBaseRdfIndexedGraph.class ) ;
	       	ts.addTestSuite( TestHBaseRdfReifier.TestHBaseRdfIndexedReifier.class ) ;
    	}

    	if( includeVPIndexed )
    	{
	       	ts.addTestSuite( TestHBaseRdfModel.TestHBaseRdfVPIndexedModel.class ) ;
	       	ts.addTestSuite( TestHBaseRdfGraph.TestHBaseRdfVPIndexedGraph.class ) ;
	       	ts.addTestSuite( TestHBaseRdfReifier.TestHBaseRdfVPIndexedReifier.class ) ;
    	}
    	
    	if( includeHybrid )
    	{
	       	ts.addTestSuite( TestHBaseRdfModel.TestHBaseRdfHybridModel.class ) ;
	       	ts.addTestSuite( TestHBaseRdfGraph.TestHBaseRdfHybridGraph.class ) ;
	       	ts.addTestSuite( TestHBaseRdfReifier.TestHBaseRdfHybridReifier.class ) ;
    	}

    	if( includeHash )
    	{
	       	ts.addTestSuite( TestHBaseRdfModel.TestHBaseRdfHashModel.class ) ;
	       	ts.addTestSuite( TestHBaseRdfGraph.TestHBaseRdfHashGraph.class ) ;
	       	ts.addTestSuite( TestHBaseRdfReifier.TestHBaseRdfHashReifier.class ) ;
    	}
    	
        return ts ;
    }
}