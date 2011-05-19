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

package com.talis.hbase.rdf.assembler;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.assemblers.AssemblerGroup;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.talis.hbase.rdf.HBaseRdf;
import com.talis.hbase.rdf.util.Vocab;

public class AssemblerVocab 
{
    private static final String NS = HBaseRdf.namespace ;
    
    public static String getURI() { return NS ; } 

    // Types
    public static final Resource HBaseConnectionAssemblerType   = Vocab.type( NS, "HBaseConnection" ) ;
    public static final Resource StoreAssemblerType             = Vocab.type( NS, "Store" ) ;
    public static final Resource DatasetAssemblerType           = Vocab.type( NS, "DatasetStore" ) ;
    public static final Resource ModelType                      = Vocab.type( NS, "Model" ) ;
    public static final Resource GraphType                      = Vocab.type( NS, "Graph" ) ;

    public static final Property pStore              			= Vocab.property( NS, "store" ) ;
    public static final Property pDataset            			= Vocab.property( NS, "dataset" ) ;
    public static final Property pGraphData          			= Vocab.property( NS, "graph" ) ;
    public static final Property pNamedGraph1        			= Vocab.property( NS, "graphName" ) ;
    public static final Property pNamedGraph2        			= Vocab.property( NS, "namedGraph" ) ;

    // ---- Store
    public static final Property pName							= Vocab.property( NS, "name" ) ;
    public static final Property pLayout             			= Vocab.property( NS, "layout" ) ;
    public static final Property pConnection         			= Vocab.property( NS, "connection" ) ;

    // ---- Connection
    public static final Property pHBaseRdfConfiguration         = Vocab.property( NS, "configuration" ) ;
    
    // ---- Query
    public static final Resource QueryAssemblerType             = Vocab.type( NS, "Query" ) ;

    public static final Property pQuery              			= Vocab.property( NS, "query" ) ;
    public static final Property pQueryFile          			= Vocab.property( NS, "queryFile" ) ;
    public static final Property pQueryString        			= Vocab.property( NS, "queryString" ) ;

    public static final Property pOutputFormat       			= Vocab.property( NS, "outputFormat" ) ;
    
    private static boolean initialized = false ; 
    
    static { init() ; }
    
    static public void init()
    {
        if ( initialized )
            return ;
        register( Assembler.general ) ;
        initialized = true ;
    }
    
    static public void register( AssemblerGroup g )
    {
        // Wire in the extension assemblers (extensions relative to the Jena assembler framework)
        //assemblerClass(CommandAssemblerType,          new CommandAssembler()) ;
        assemblerClass( g, QueryAssemblerType,            new QueryAssembler() ) ;
        assemblerClass( g, HBaseConnectionAssemblerType,  new HBaseRdfConnectionDescAssembler() ) ;
        assemblerClass( g, StoreAssemblerType,            new StoreDescAssembler() ) ;
        assemblerClass( g, DatasetAssemblerType,          new DatasetStoreAssembler() ) ;
        assemblerClass( g, ModelType,                     new HBaseRdfModelAssembler() ) ;
        assemblerClass( g, GraphType,                     new HBaseRdfModelAssembler() ) ;
    }
    
    private static void assemblerClass( AssemblerGroup g, Resource r, Assembler a )
    {
        if ( g == null )
            g = Assembler.general ;
        g.implementWith( r, a ) ;
    }
}