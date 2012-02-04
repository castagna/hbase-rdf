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

package com.talis.hbase.rdf.assembler;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.StoreDesc;

public class HBaseRdfModelAssembler extends AssemblerBase implements Assembler
{
    DatasetStoreAssembler datasetAssem = new DatasetStoreAssembler() ;
    
    @Override
    public Model open(Assembler a, Resource root, Mode mode)
    {
        // Make a model.
        // [] rdf:type hbaserdf:Model ;
        //    hbaserdf:dataset <dataset> ;
        //    hbaserdf:graphName <someURI> .
        
        // A model (graph) is a (dataset, name) pair where the name can be absent
        // meaning the default graph of the dataset.
        
        Resource dataset = GraphUtils.getResourceValue( root, AssemblerVocab.pDataset ) ;
        if ( dataset == null )
            throw new MissingException( root, "No dataset for model or graph" ) ;
        StoreDesc storeDesc = datasetAssem.openStore( a, dataset, mode ) ;

        // Attempt to find a graph name - may be absent.
        // Two names : "namedGraph" and "graphName"
        String x = GraphUtils.getAsStringValue( root, AssemblerVocab.pNamedGraph1 ) ;
        if ( x == null )
            x = GraphUtils.getAsStringValue( root, AssemblerVocab.pNamedGraph2 ) ;
        
        // No name - default model.
        if ( x == null )
            return HBaseRdfFactory.connectDefaultModel( storeDesc ) ;
        else
            return HBaseRdfFactory.connectNamedModel( storeDesc, x ) ;
    }
}