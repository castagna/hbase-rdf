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

package com.talis.hbase.rdf.test.misc;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.sse.SSE;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionFactory;
import com.talis.hbase.rdf.store.StoreFactory;
import com.talis.hbase.rdf.test.junit.ParamAllStores;

import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestConnectionPooled extends ParamAllStores
{
    Configuration config ;
    
    @Parameters
    public static Collection<Object[]> configs()
    {
    	return Arrays.asList( new Object[][]
    	{
    		{ "simple", 	StoreFactory.create( StoreDesc.read( "testing/StoreDesc/simple.ttl" ) ) },
    		{ "vert-part", 	StoreFactory.create( StoreDesc.read( "testing/StoreDesc/vertical-partitioning.ttl" ) ) }
    	} ) ;
    }
    
    // Use "AllStores" so the JDBC connections are already there
    public TestConnectionPooled( String name, Store store )
    {
        super( name, store ) ;
    }
    
    @Before public void before()
    {
        store.getTableFormatter().create() ;
        config = store.getConnection().getConfiguration() ;
    }

    @After public void after() { }

    
    @Test public void reuseHBaseRdfConection() 
    {
        Triple t1 = SSE.parseTriple( "(:x1 :p :z)" ) ;
        Triple t2 = SSE.parseTriple( "(:x2 :p :z)" ) ;
        
        // Make store.
        {
            HBaseRdfConnection sConn1 = HBaseRdfConnectionFactory.create( config ) ;
            Store store1 = StoreFactory.create( sConn1, store.getLayoutType(), store.getStoreName() ) ;
            
            Graph graph1 = HBaseRdfFactory.connectDefaultGraph( store1, ReificationStyle.Standard ) ;
            graph1.add( t1 ) ;
            assertTrue( graph1.contains( t1 ) ) ;            
        }       
        
        // Mythically return conn to the pool.
        // Get from pool
        // i.e. same connection.  Make a store around it
        
        {
            HBaseRdfConnection sConn2 = HBaseRdfConnectionFactory.create( config ) ;
            Store store2 = StoreFactory.create( sConn2, store.getLayoutType(), store.getStoreName() ) ;

            Graph graph2 = HBaseRdfFactory.connectDefaultGraph( store2, ReificationStyle.Standard ) ;
            assertTrue( graph2.contains( t1 ) ) ;
            
            graph2.add( t2 ) ;
            assertTrue( graph2.contains( t2 ) ) ;
        }
        //System.exit(0) ;
    }
}