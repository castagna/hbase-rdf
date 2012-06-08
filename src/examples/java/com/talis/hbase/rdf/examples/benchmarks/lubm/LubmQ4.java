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

import org.mindswap.pellet.jena.PelletReasonerFactory;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

public class LubmQ4 
{
	public static void main(String[] args)
	{
		Store store = HBaseRdfFactory.connectStore( args[0] ) ;
		Model schema = HBaseRdfFactory.connectNamedModel( store, "http://cs.utdallas.edu/hbase-rdf/bm#LUBM" ) ;
		try
		{
			long count = 0L;
			OntModel m = ModelFactory.createOntologyModel( PelletReasonerFactory.THE_SPEC, schema );
			String queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X ?Y1 ?Y2 ?Y3 " +
				" WHERE " +
				" { " +
				"		?X rdf:type ub:Professor . " +
				"		?X ub:worksFor <http://www.Department0.University0.edu> . " +
				"		?X ub:name ?Y1 . " +
				"		?X ub:emailAddress ?Y2 . " +
				"		?X ub:telephone ?Y3 " +
				" } ";

			Query query = QueryFactory.create(queryString);
			QueryExecution qexec = QueryExecutionFactory.create(query, m);
			ResultSet rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);
		}
		catch(Exception e) { e.printStackTrace(); }
	}
}
