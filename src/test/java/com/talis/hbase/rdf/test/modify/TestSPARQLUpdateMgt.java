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

package com.talis.hbase.rdf.test.modify;

import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.modify.TestUpdateGraphMgt;
import com.hp.hpl.jena.update.GraphStore;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.test.StoreCreator;

import junit.framework.TestSuite;

public class TestSPARQLUpdateMgt
{
    public static junit.framework.Test suite() 
    {
        TestSuite ts = new TestSuite() ;
        
        ts.addTestSuite( TestSPARQLUpdateMgtSimple.class ) ;
        ts.addTestSuite( TestSPARQLUpdateMgtVertPart.class ) ;
        ts.addTestSuite( TestSPARQLUpdateMgtIndexed.class ) ;
        ts.addTestSuite( TestSPARQLUpdateMgtVPIndexed.class ) ;
        ts.addTestSuite( TestSPARQLUpdateMgtHybrid.class ) ;
        ts.addTestSuite( TestSPARQLUpdateMgtHash.class ) ;
        
        return ts ;
    }
    
    public static class TestSPARQLUpdateMgtSimple extends TestUpdateGraphMgt
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreSimple() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }
    }
    
    public static class TestSPARQLUpdateMgtVertPart extends TestUpdateGraphMgt
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreVerticallyPartitioned() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }    	
    }
    
    public static class TestSPARQLUpdateMgtIndexed extends TestUpdateGraphMgt
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreIndexed() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }
    }

    public static class TestSPARQLUpdateMgtVPIndexed extends TestUpdateGraphMgt
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreVPIndexed() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }
    }

    public static class TestSPARQLUpdateMgtHybrid extends TestUpdateGraphMgt
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreHybrid() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }
    }

    public static class TestSPARQLUpdateMgtHash extends TestUpdateGraphMgt
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreHash() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }
    }
}