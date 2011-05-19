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

package com.talis.hbase.rdf.store;

import java.util.List;

import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.util.Named;
import com.hp.hpl.jena.sparql.util.Symbol;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.shared.SymbolRegistry;

public class LayoutType extends Symbol implements Named
{
    public static final LayoutType LayoutSimple	 	= new LayoutType( "layout1" ) ;
    public static final LayoutType LayoutVertPart	= new LayoutType( "layout2" ) ;

    static SymbolRegistry<LayoutType> registry = new SymbolRegistry<LayoutType>() ;
    static { init() ; }
    
    public static LayoutType fetch( String layoutTypeName )
    {
        if ( layoutTypeName == null )
            throw new IllegalArgumentException( "LayoutType.convert: null not allowed" ) ;
        
        LayoutType t = registry.lookup( layoutTypeName ) ;
        if ( t != null ) return t ;

        LoggerFactory.getLogger( LayoutType.class ).warn( "Can't turn '" + layoutTypeName + "' into a layout type" ) ;
        throw new HBaseRdfException( "Can't turn '" + layoutTypeName + "' into a layout type" ) ; 
    }
    
    static void init()
    {
        register( LayoutSimple ) ;
        register( LayoutVertPart ) ;
    }
    
    static public List<String> allNames() { return registry.allNames() ; }
    static public List<LayoutType> allTypes() { return registry.allSymbols() ; }
    
    static public void register( String name )
    {
        if ( name == null )
            throw new IllegalArgumentException( "LayoutType.register(String): null not allowed" ) ;
        register( new LayoutType( name ) ) ; 
    }
    
    static public void register( LayoutType layoutType )
    {
        if ( layoutType == null )
            throw new IllegalArgumentException( "LayoutType.register(LayoutType): null not allowed" ) ;
        registry.register( layoutType ) ; 
    }

    static public void registerName( String layoutName, LayoutType layoutType )
    {
        if ( layoutType == null )
            throw new IllegalArgumentException( "LayoutType.registerName: null not allowed" ) ;
        registry.register( layoutName, layoutType ) ; 
    }

    private LayoutType( String layoutName ) { super( layoutName ) ; }

    public String getName() { return super.getSymbol() ; }
}