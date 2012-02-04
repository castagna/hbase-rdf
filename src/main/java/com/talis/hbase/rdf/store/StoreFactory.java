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

package com.talis.hbase.rdf.store;

import static com.talis.hbase.rdf.store.LayoutType.* ;
import static java.lang.String.format ;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hbase.rdf.HBaseRdf;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionFactory;
import com.talis.hbase.rdf.layout.hash.StoreHash;
import com.talis.hbase.rdf.layout.hybrid.StoreHybrid;
import com.talis.hbase.rdf.layout.indexed.StoreIndexed;
import com.talis.hbase.rdf.layout.simple.StoreSimple;
import com.talis.hbase.rdf.layout.verticalpartitioning.StoreVP;
import com.talis.hbase.rdf.layout.vpindexed.StoreVPIndexed;

public class StoreFactory 
{
    private static Logger log = LoggerFactory.getLogger( StoreFactory.class ) ;
    
    static { HBaseRdf.init() ; } 

    public static Store create( String filename )
    { return create( StoreDesc.read( filename ), null ) ; }
    
    
    public static Store create( HBaseRdfConnection hbase, LayoutType layout, String name )
    { 
        StoreDesc desc = new StoreDesc( layout, name ) ;
        return create( desc, hbase ) ;
    }

    public static Store create( LayoutType layout, String name )
    { 
        StoreDesc desc = new StoreDesc( layout, name ) ;
        return create( desc, null ) ;
    }

    public static Store create( StoreDesc desc )
    { return create( desc, null ) ; }
    
    public static Store create( StoreDesc desc, HBaseRdfConnection hbase )
    {
        Store store = _create( hbase, desc ) ;
        return store ;
    }
    
    private static Store _create( HBaseRdfConnection hbase, StoreDesc desc )
    {
        if ( hbase == null && desc.connDesc == null )
            desc.connDesc = HBaseRdfConnectionDesc.none() ;

        if ( hbase == null && desc.connDesc != null)
            hbase = HBaseRdfConnectionFactory.create( desc.connDesc ) ;
        
        LayoutType layoutType = desc.getLayout() ;
        
        return _create( desc, hbase, layoutType ) ;
    }

    private static Store _create( StoreDesc desc, HBaseRdfConnection hbase, LayoutType layoutType )
    {
        StoreMaker f = registry.get( layoutType ) ;
        if ( f == null )
        {
            log.warn( format( "No factory for %s", layoutType.getName() ) ) ;
            return null ;
        }       
        return f.create( hbase, desc ) ;
    }
 
    public static void register( LayoutType layoutType, StoreMaker factory )
    {
        registry.put( layoutType, factory ) ;
    }
    
    private static Map<LayoutType, StoreMaker> registry = new HashMap<LayoutType, StoreMaker>() ;

    static { setRegistry() ; checkRegistry() ; }

    static private void setRegistry()
    {
        register( LayoutSimple, 
                	new StoreMaker() {
                    public Store create( HBaseRdfConnection conn, StoreDesc desc )
                    { return new StoreSimple( conn, desc ) ; } } ) ;
        
        register( LayoutVertPart, 
        			new StoreMaker() {
        			public Store create( HBaseRdfConnection conn, StoreDesc desc )
        			{ return new StoreVP( conn, desc ) ; } } ) ;
        
        register( LayoutIndexed, 
        			new StoreMaker() {
        			public Store create( HBaseRdfConnection conn, StoreDesc desc )
        			{ return new StoreIndexed( conn, desc ) ; } } ) ;
        
        register( LayoutVPIndexed,
        			new StoreMaker() {
        			public Store create( HBaseRdfConnection conn, StoreDesc desc )
        			{ return new StoreVPIndexed( conn, desc ) ; } } ) ;
        
        register( LayoutHybrid, 
        			new StoreMaker() {
        			public Store create( HBaseRdfConnection conn, StoreDesc desc )
        			{ return new StoreHybrid( conn, desc ) ; } } ) ;
        
        register( LayoutHash, 
    				new StoreMaker() {
    				public Store create( HBaseRdfConnection conn, StoreDesc desc )
    			{ 	return new StoreHash( conn, desc ) ; } } ) ;
    }
    
    static private void checkRegistry()
    {
        LayoutType[] layoutTypes = { LayoutSimple, LayoutVertPart } ;
        
        Set<StoreMaker> seen = new HashSet<StoreMaker>() ;
        
        for ( LayoutType k1 : layoutTypes )
        {
            if ( !registry.containsKey( k1 ) )
                log.warn( format( " Missing store maker: %s ", k1.getName() ) ) ;
            StoreMaker x = registry.get( k1 ) ;
            if ( seen.contains( x ) )
            	log.warn( format( " Duplicate store maker: %s ", k1.getName() ) ) ;
            seen.add( x ) ;
        }
    }
}