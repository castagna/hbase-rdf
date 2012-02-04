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

package com.talis.hbase.rdf;

import org.apache.hadoop.conf.Configuration;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.update.GraphStore;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionFactory;
import com.talis.hbase.rdf.graph.GraphHBaseRdfBase;
import com.talis.hbase.rdf.store.DatasetStore;
import com.talis.hbase.rdf.store.DatasetStoreHBaseRdfGraph;
import com.talis.hbase.rdf.store.StoreFactory;

public class HBaseRdfFactory 
{
    // ---- Connections
    public static HBaseRdfConnection createConnection( String configFile, boolean isAssemblerFile )
    { return HBaseRdfConnectionFactory.create( configFile, isAssemblerFile ) ; } 
    
    public static HBaseRdfConnection createConnection( HBaseRdfConnectionDesc desc )
    { return HBaseRdfConnectionFactory.create( desc ) ; }

    public static HBaseRdfConnection createConnection( Configuration config )
    { return HBaseRdfConnectionFactory.create( config ) ; }

    public static Configuration createHBaseConfiguration( String configFile )
    { return HBaseRdfConnectionFactory.createHBaseConfiguration( configFile ) ; }
    
    public static Store connectStore( String configFile ) 
    { return StoreFactory.create( configFile ) ; }
    
    public static Store connectStore( StoreDesc desc ) 
    { return StoreFactory.create( desc ) ; }

    public static Store connectStore( HBaseRdfConnection hbaseConnection, StoreDesc desc ) 
    { return StoreFactory.create( desc, hbaseConnection ) ; }
    
    public static Store connectStore( Configuration config, StoreDesc desc ) 
    {
        HBaseRdfConnection sdb = HBaseRdfConnectionFactory.create( config ) ;
        return StoreFactory.create( desc, sdb ) ;
    }

    // ---- Dataset

    public static Dataset connectDataset( Store store )
    { return DatasetStore.create( store ) ; }

    public static Dataset connectDataset( StoreDesc desc )
    { return DatasetStore.create( connectStore( desc ) ) ; }

    public static Dataset connectDataset( HBaseRdfConnection hbaseConnection, StoreDesc desc )
    { return DatasetStore.create( connectStore( hbaseConnection, desc ) ) ; }
    
    public static Dataset connectDataset( Configuration config, StoreDesc desc )
    { return DatasetStore.create( connectStore( config, desc ) ) ; }
    
    public static Dataset connectDataset( String configFile )
    { return DatasetStore.create( connectStore( configFile ) ) ; }
    
    // ---- GraphStore
    
    public static GraphStore connectGraphStore( Store store, ReificationStyle style )
    { return new DatasetStoreHBaseRdfGraph( store, HBaseRdf.getContext().copy(), style ) ; }

    public static GraphStore connectGraphStore( StoreDesc desc )
    { return connectGraphStore( connectStore( desc ), ReificationStyle.Standard ) ; }

    public static GraphStore connectGraphStore( StoreDesc desc, ReificationStyle style )
    { return connectGraphStore( connectStore( desc ), style ) ; }

    public static GraphStore connectGraphStore( HBaseRdfConnection hbaseConnection, StoreDesc desc )
    { return connectGraphStore( connectStore( hbaseConnection, desc ), ReificationStyle.Standard ) ; }

    public static GraphStore connectGraphStore( HBaseRdfConnection hbaseConnection, StoreDesc desc, ReificationStyle style )
    { return connectGraphStore( connectStore( hbaseConnection, desc ), style ) ; }

    public static GraphStore connectGraphStore( Configuration config, StoreDesc desc )
    { return connectGraphStore( connectStore( config, desc ), ReificationStyle.Standard ) ; }

    public static GraphStore connectGraphStore( Configuration config, StoreDesc desc, ReificationStyle style )
    { return connectGraphStore( connectStore( config, desc ), style ) ; }

    public static GraphStore connectGraphStore( String configFile )
    { return connectGraphStore( connectStore( configFile ), ReificationStyle.Standard ) ; }

    public static GraphStore connectGraphStore( String configFile, ReificationStyle style )
    { return connectGraphStore( connectStore( configFile ), style ) ; }

    // ---- Graph
    
    public static Graph connectDefaultGraph( String configFile )
    { return connectDefaultGraph( StoreFactory.create( configFile ), ReificationStyle.Standard ) ; }

    public static Graph connectDefaultGraph( String configFile, ReificationStyle style )
    { return connectDefaultGraph( StoreFactory.create( configFile ), style ) ; }

    public static Graph connectDefaultGraph( StoreDesc desc )
    { return connectDefaultGraph( StoreFactory.create( desc ), ReificationStyle.Standard ) ; }

    public static Graph connectDefaultGraph( StoreDesc desc, ReificationStyle style )
    { return connectDefaultGraph( StoreFactory.create( desc ), style ) ; }

    public static Graph connectDefaultGraph( Store store, ReificationStyle style )
    { return new GraphHBaseRdfBase( store, style ) ; }

    public static Graph connectNamedGraph( String configFile, String iri )
    { return connectNamedGraph( StoreFactory.create( configFile ), iri, ReificationStyle.Standard ) ; }

    public static Graph connectNamedGraph( String configFile, String iri, ReificationStyle style )
    { return connectNamedGraph( StoreFactory.create( configFile ), iri, style ) ; }

    public static Graph connectNamedGraph( StoreDesc desc, String iri )
    { return connectNamedGraph( StoreFactory.create( desc ), iri, ReificationStyle.Standard ) ; }

    public static Graph connectNamedGraph( StoreDesc desc, String iri, ReificationStyle style )
    { return connectNamedGraph( StoreFactory.create( desc ), iri, style ) ; }

    public static Graph connectNamedGraph( Store store, String iri, ReificationStyle style )
    { return new GraphHBaseRdfBase( store, iri, style ) ; }

    public static Graph connectNamedGraph( String configFile, Node node )
    { return connectNamedGraph( StoreFactory.create( configFile ), node, ReificationStyle.Standard ) ; }

    public static Graph connectNamedGraph( String configFile, Node node, ReificationStyle style )
    { return connectNamedGraph( StoreFactory.create( configFile ), node, style ) ; }

    public static Graph connectNamedGraph( StoreDesc desc, Node node )
    { return connectNamedGraph( StoreFactory.create( desc ), node, ReificationStyle.Standard ) ; }

    public static Graph connectNamedGraph( StoreDesc desc, Node node, ReificationStyle style )
    { return connectNamedGraph( StoreFactory.create( desc ), node, style ) ; }

    public static Graph connectNamedGraph(Store store, Node node, ReificationStyle style  )
    { return new GraphHBaseRdfBase( store, node, style ) ; }
    
    
    // ---- Model
    
    public static Model connectDefaultModel( String configFile )
    { return connectDefaultModel( StoreFactory.create( configFile ) ) ; }

    /**
     * Connect to the default model in a store
     * @param desc
     * @return Model
     */
    public static Model connectDefaultModel( StoreDesc desc )
    { return connectDefaultModel( StoreFactory.create( desc ) ) ; }

    /**
     * Connect to the default model in a store
     * @param store
     * @return Model
     */
    public static Model connectDefaultModel( Store store )
    { return createModelHBaseRdf( store, ReificationStyle.Standard ) ; }

    public static Model connectDefaultModel( Store store, ReificationStyle style )
    { return createModelHBaseRdf( store, style ) ; }

    /**
     * Connect to the named model in a store
     * @param desc
     * @param iri
     * @return Model
     */
    public static Model connectNamedModel( StoreDesc desc, String iri )
    { return connectNamedModel( StoreFactory.create( desc ), iri, ReificationStyle.Standard ) ; }

    public static Model connectNamedModel( StoreDesc desc, String iri, ReificationStyle style )
    { return connectNamedModel( StoreFactory.create( desc ), iri, style ) ; }

    /**
     * Connect to the named model in a store
     * @param store
     * @param iri
     * @return Model
     */
    public static Model connectNamedModel( Store store, String iri, ReificationStyle style )
    { return createModelHBaseRdf( store, iri, style ) ; }

    public static Model connectNamedModel( Store store, String iri )
    { return createModelHBaseRdf( store, iri, ReificationStyle.Standard ) ; }
    
    /**
     * Connect to the named model in a store
     * @param configFile
     * @param iri
     * @return Model
     */
    public static Model connectNamedModel( String configFile, String iri )
    { return connectNamedModel( StoreFactory.create( configFile ), iri, ReificationStyle.Standard ) ; }

    public static Model connectNamedModel( String configFile, String iri, ReificationStyle style )
    { return connectNamedModel( StoreFactory.create( configFile ), iri, style ) ; }

    /**
     * Connect to the named model in a store
     * @param desc
     * @param resource
     * @return Model
     */
    public static Model connectNamedModel( StoreDesc desc, Resource resource )
    { return connectNamedModel( StoreFactory.create( desc ), resource, ReificationStyle.Standard ) ; }

    public static Model connectNamedModel( StoreDesc desc, Resource resource, ReificationStyle style )
    { return connectNamedModel( StoreFactory.create( desc ), resource, style ) ; }

    /**
     * Connect to the named model in a store
     * @param store
     * @param resource
     * @return Model
     */
    public static Model connectNamedModel( Store store, Resource resource )
    { return createModelHBaseRdf( store, resource, ReificationStyle.Standard ) ; }

    public static Model connectNamedModel( Store store, Resource resource, ReificationStyle style )
    { return createModelHBaseRdf( store, resource, style ) ; }

    /**
     * Connect to the named model in a store
     * @param configFile
     * @param resource
     * @return Model
     */
    public static Model connectNamedModel( String configFile, Resource resource )
    { return connectNamedModel( StoreFactory.create( configFile ), resource ) ; }

    // ---- Workers
    
    private static Model createModelHBaseRdf( Store store, ReificationStyle style )
    { return ModelFactory.createModelForGraph( new GraphHBaseRdfBase( store, style ) ) ; }
    
    private static Model createModelHBaseRdf( Store store, String iri, ReificationStyle style )
    { return ModelFactory.createModelForGraph( new GraphHBaseRdfBase( store, iri, style ) ) ; }

    private static Model createModelHBaseRdf( Store store, Resource resource, ReificationStyle style )
    { return ModelFactory.createModelForGraph( new GraphHBaseRdfBase( store, resource.asNode(), style ) ) ; }
}