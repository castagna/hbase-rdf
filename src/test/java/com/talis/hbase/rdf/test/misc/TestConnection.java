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

package com.talis.hbase.rdf.test.misc;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.store.StoreFactory;
import com.talis.hbase.rdf.test.junit.ParamAllStoreDesc;

import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestConnection extends ParamAllStoreDesc
{
    Configuration config ;
    
    @Parameters
    public static Collection<Object[]> configs()
    {
    	return Arrays.asList( new Object[][]
    	{
    		{ "simple", 		StoreDesc.read( "testing/StoreDesc/simple.ttl" ) },
    		{ "vert-part", 		StoreDesc.read( "testing/StoreDesc/vertical-partitioning.ttl" ) },
    		{ "indexed", 		StoreDesc.read( "testing/StoreDesc/indexed.ttl" ) },
    		{ "vp-indexed", 	StoreDesc.read( "testing/StoreDesc/vp-indexed.ttl" ) },
    		{ "hybrid", 		StoreDesc.read( "testing/StoreDesc/hybrid.ttl" ) },
    		{ "hash", 			StoreDesc.read( "testing/StoreDesc/hash.ttl" ) }
    	} ) ;
    }
    
    public TestConnection( String name, StoreDesc storeDesc )
    {
        super( name, storeDesc ) ;
    }

    @Before public void before()
    {
        config = HBaseRdfFactory.createHBaseConfiguration( storeDesc.connDesc.getConfig() ) ;
    }
    
    @Test public void connection_1()
    {
        HBaseRdfConnection conn1 = HBaseRdfFactory.createConnection( config ) ;
        Store store1 = StoreFactory.create( storeDesc, conn1 ) ;
        
        // Reset
        store1.getTableFormatter().format();
        
        HBaseRdfConnection conn2 = HBaseRdfFactory.createConnection( config ) ;
        Store store2 = StoreFactory.create( storeDesc, conn2 ) ;
        
        Model model1 = HBaseRdfFactory.connectDefaultModel( store1 ) ;
        Model model2 = HBaseRdfFactory.connectDefaultModel( store2 ) ;
        
        Resource s = model1.createResource() ;
        Property p = model1.createProperty( "http://example/p" ) ;
        
        // These are autocommit so two stores should be OK (but not a good design paradigm)
        model1.add( s, p, "model1" ) ;
        model2.add( s, p, "model2" ) ;
        
        assertEquals( 2, model1.size() ) ;
        assertEquals( 2, model2.size() ) ;
        //assertTrue( model1.isIsomorphicWith( model2 ) ) ;       
    }
}