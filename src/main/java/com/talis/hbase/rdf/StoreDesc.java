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

package com.talis.hbase.rdf;

import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.hp.hpl.jena.util.FileManager;
import com.talis.hbase.rdf.assembler.AssemblerVocab;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionDesc;
import com.talis.hbase.rdf.store.LayoutType;

public class StoreDesc 
{
    public HBaseRdfConnectionDesc connDesc   	= null ;
    private LayoutType layout             		= null ;
    private String name							= null ;
    
    public static StoreDesc read( String filename )
    {
        Model m = FileManager.get().loadModel( filename ) ;
        return read( m ) ;
    }
    
    public StoreDesc( String layoutName, String name )
    {
        this( LayoutType.fetch( layoutName ), name ) ;
    }
    
    public StoreDesc( LayoutType layout, String name )
    {
        this.layout = layout ;
        if( name == null ) this.name = "default" ;
        else this.name = name ;
    }
    
    public String getStoreName() { return name ; }
    
    public void setStoreName( String name ) { this.name = name ; }
    
    public LayoutType getLayout() { return layout ; }
    
    public void setLayout( LayoutType layout ) { this.layout = layout ; }

    public static StoreDesc read( Model m )
    {
        // Does not mind store descriptions or dataset descriptions
        Resource r = GraphUtils.getResourceByType( m, AssemblerVocab.StoreAssemblerType ) ;
        
        if ( r == null )
            throw new HBaseRdfException( "Can't find store description" ) ;
        return read( r ) ;
    }

    public static StoreDesc read( Resource r )
    {
        return (StoreDesc)AssemblerBase.general.open( r ) ;
    }
}