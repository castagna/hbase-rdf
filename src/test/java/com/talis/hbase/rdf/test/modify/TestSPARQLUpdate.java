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

package com.talis.hbase.rdf.test.modify;

import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.update.GraphStore;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.test.StoreCreator;

import junit.framework.TestSuite;

public class TestSPARQLUpdate
{
    public static junit.framework.Test suite() 
    {
        TestSuite ts = new TestSuite() ;
        
        ts.addTestSuite( TestSPARQLUpdateSimple.class ) ;
        ts.addTestSuite( TestSPARQLUpdateVertPart.class ) ;
        
        return ts ;
    }
    
    public static class TestSPARQLUpdateSimple extends AbstractTestSPARQLUpdate
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreSimple() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }
    }
    
    public static class TestSPARQLUpdateVertPart extends AbstractTestSPARQLUpdate
    {
	    @Override
	    protected GraphStore getEmptyGraphStore()
	    {
	        Store store = StoreCreator.getStoreVerticallyPartitioned() ;
	        GraphStore graphStore = HBaseRdfFactory.connectGraphStore( store, ReificationStyle.Standard ) ;
	        return graphStore ;
	    }    	
    }
}
