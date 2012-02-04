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

package com.talis.hbase.rdf.util;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.util.FileManager;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

public class StoreUtils 
{
    public static void load( Store store, String filename )
    {
        Model model = HBaseRdfFactory.connectDefaultModel( store ) ;
        FileManager.get().readModel( model, filename ) ;
    }

    public static void load( Store store, String filename, ReificationStyle style )
    {
        Model model = HBaseRdfFactory.connectDefaultModel( store, style ) ;
        FileManager.get().readModel( model, filename ) ;
    }

    public static void load( Store store, String graphIRI, String filename )
    {
        Model model = HBaseRdfFactory.connectNamedModel( store, graphIRI, ReificationStyle.Standard ) ;
        FileManager.get().readModel( model, filename ) ;
    }

    public static void load( Store store, String graphIRI, String filename, ReificationStyle style )
    {
        Model model = HBaseRdfFactory.connectNamedModel( store, graphIRI, style ) ;
        FileManager.get().readModel( model, filename ) ;
    }

    public static Iterator<Node> storeGraphNames( Store store )
    {
    	return store.getConfig().listGraphNodes() ;
    }
    
    public static boolean containsGraph( Store store, Node graphNode )
    {
    	return store.getConfig().containsGraph( graphNode ) ;
    }    
}