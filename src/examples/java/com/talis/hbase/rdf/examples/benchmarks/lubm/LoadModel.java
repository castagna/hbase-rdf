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

package com.talis.hbase.rdf.examples.benchmarks.lubm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.mindswap.pellet.jena.PelletReasonerFactory;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

public class LoadModel 
{
	public static void main( String[] args ) throws IOException, InterruptedException
	{
		int numOfRuns = new Integer( args[0] ).intValue() ;
		
		for( int x = 1 ; x <= numOfRuns ; x++ )
		{
			Store store = HBaseRdfFactory.connectStore( args[1] ) ;
			if( args[2].equals( "--format=true" ) ) store.getTableFormatter().format() ;
			Model schema = HBaseRdfFactory.connectNamedModel( store, args[3] ) ;
			long startTime = System.nanoTime() ;
			schema.read( "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl" );
	
			OntModel m = ModelFactory.createOntologyModel( PelletReasonerFactory.THE_SPEC, schema );
			String sInputDirectory = args[4];
			File inputDirectory = new File(sInputDirectory);
			String[] sFilenames = inputDirectory.list(new OWLFilenameFilter());
			for (int i = 0; i < sFilenames.length; i++) 
			{
				InputStream in = FileManager.get().open(sInputDirectory+sFilenames[i]);
				if (in == null) { throw new IllegalArgumentException( "File: " + sFilenames[i] + " not found"); }
				m.read( in, "http://www.utdallas.edu/benchmark-test#", "RDF/XML-ABBREV");
				in.close();
			}
			schema.close() ;
			long endTime = System.nanoTime() ;
			System.out.println( "Time to load:: " + ( endTime - startTime ) * 1e-6 + " ms." ) ;
			Thread.sleep( 10000 ) ;
		}
	}
}
