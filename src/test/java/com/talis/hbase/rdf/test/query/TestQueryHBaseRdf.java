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

package com.talis.hbase.rdf.test.query;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.sparql.engine.QueryEngineFactory;
import com.hp.hpl.jena.sparql.engine.QueryExecutionBase;
import com.hp.hpl.jena.sparql.engine.ref.QueryEngineRef;
import com.hp.hpl.jena.sparql.junit.EarlReport;
import com.hp.hpl.jena.sparql.junit.EarlTestCase;
import com.hp.hpl.jena.sparql.junit.TestItem;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.store.DatasetStore;
import com.talis.hbase.rdf.test.junit.StoreList;
import com.talis.hbase.rdf.util.StoreUtils;

public class TestQueryHBaseRdf extends EarlTestCase
{
    public static boolean VERBOSE = false ;
    StoreDesc storeDesc ;
    Store store = null ;
    TestItem item ;
    private static Logger log = LoggerFactory.getLogger( TestQueryHBaseRdf.class ) ; 
    
    public TestQueryHBaseRdf( StoreDesc desc, String testName, EarlReport report, TestItem item )
    {
        super( testName, item.getURI(), report ) ;
        this.storeDesc = desc ;
        this.item = item ;
    }

    // NB static.
    // Assumes that tests run serially
    
    static String currentTestName = null ;
    static List<String> lastDftLoaded = new ArrayList<String>() ;
    static List<String> lastNamedLoaded = new ArrayList<String>() ;
    
    boolean skipThisTest = false ;

    @Override
    public void setUp()
    { 
        if ( currentTestName != null )
        {
            log.warn( this.getName() + " : Already in test '" + currentTestName + "'" ) ;
            skipThisTest = true ;
            return ;
        }
        
        currentTestName = getName() ;
        
        final List<String> filenamesDft = item.getDefaultGraphURIs() ;
        final List<String> filenamesNamed = item.getNamedGraphURIs() ;
        
        try { store = StoreList.testStore( storeDesc ) ; } 
        catch (Exception ex)
        {
            ex.printStackTrace(System.err) ;
            return ;
        }

        // Truncate outside a transaction.
        store.getTableFormatter().truncate() ;

        // Default graph
        for ( String fn : filenamesDft )
        	StoreUtils.load( store, fn ) ;    
            
        // Named graphs
        for ( String fn : filenamesNamed )
            StoreUtils.load( store, fn, fn ) ;    

        lastDftLoaded = filenamesDft ;
        lastNamedLoaded = filenamesNamed ;
    }
    
    @Override
    public void tearDown()
    { 
        if ( store != null )
            store.close() ;
        store = null ;
        currentTestName = null ;
    }

    @Override
    public void runTestForReal()
    {
        if ( skipThisTest )
        {
            log.info(this.getName()+" : Skipped") ;
            return ;
        }
        
        if ( store == null )
            fail("No store") ;
        
        Query query = QueryFactory.read( item.getQueryFile() ) ;
        
        // If null, then compare to running ARQ in-memory 
        if ( VERBOSE )
        {
            System.out.println( "Test: " + this.getName() ) ;
            System.out.println( query ) ;  
        }
        
        // Make sure a plain, no sameValueAs graph is used.
        Object oldValue = ARQ.getContext().get( ARQ.strictGraph ) ;
        ARQ.setTrue( ARQ.strictGraph ) ;
        Dataset ds = DatasetFactory.create( item.getDefaultGraphURIs(), item.getNamedGraphURIs() ) ;
        ARQ.getContext().set( ARQ.strictGraph, oldValue ) ;
        
        // ---- First, get the expected results by executing in-memory or from a results file.
        
        ResultSet rs = null ;
        if ( item.getResults() != null )
            rs = item.getResults().getResultSet() ;
        ResultSetRewindable rs1 = null ;
        String expectedLabel = "" ;
        if ( rs != null )
        {
            rs1 = ResultSetFactory.makeRewindable( rs ) ;
            expectedLabel = "Results file" ;
        }
        else
        {
            QueryEngineFactory f = QueryEngineRef.getFactory() ;
            QueryExecution qExec1 = new QueryExecutionBase( query, ds, null, f ) ;
            rs1 = ResultSetFactory.makeRewindable( qExec1.execSelect() ) ;
            qExec1.close() ;
            expectedLabel = "Standard engine" ;
        }
        
        // ---- Second, execute in HBaseRdf

        QueryEngineFactory f2 = QueryEngineRef.getFactory() ;
        ds = DatasetStore.create( store ) ;
        QueryExecution qExec2 = new QueryExecutionBase( query, ds, null, f2 ) ;
        
        rs = qExec2.execSelect() ;
            
        ResultSetRewindable rs2 = ResultSetFactory.makeRewindable( rs ) ;
        boolean b = ResultSetCompare.equalsByTerm( rs1, rs2 ) ;
        if ( !b )
        {
            rs1.reset() ;
            rs2.reset() ;
            System.out.println( "------------------- " + this.getName() );
            System.out.printf( "**** Expected (%s)", expectedLabel ) ;
            ResultSetFormatter.out( System.out, rs1 ) ; 
            System.out.println( "**** Got (HBaseRdf)" ) ;
            ResultSetFormatter.out( System.out, rs2 ) ;
        }
            
        assertTrue("Results sets not the same", b) ; 
    }
}