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

public class LubmAll
{
	public static void main(String[] args)
	{
		Store store = HBaseRdfFactory.connectStore( args[0] ) ;
		Model schema = HBaseRdfFactory.connectNamedModel( store, "http://cs.utdallas.edu/hbase-rdf/bm#LUBM" ) ;
		try
		{
			long count = 0L;

			OntModel m = ModelFactory.createOntologyModel( PelletReasonerFactory.THE_SPEC, schema );

			//Query 1 run to do classification and realization
			String queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " + 
				" SELECT * WHERE " +
				" {	" +
				"		?x rdf:type ub:GraduateStudent . " +
				"		?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> . " +
				" }";

			Query query = QueryFactory.create(queryString);
			QueryExecution qexec = QueryExecutionFactory.create(query, m);
			ResultSet rs = qexec.execSelect();
			while( rs.hasNext() )
			{ rs.nextSolution(); }
			qexec.close();

			//Query 1
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " + 
				" SELECT * WHERE " +
				" {	" +
				"		?x rdf:type ub:GraduateStudent . " +
				"		?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> . " +
				" }";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 2
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X ?Y ?Z " +
				" WHERE " +
				" { " +
				"		?X rdf:type ub:GraduateStudent . " +
				"		?Y rdf:type ub:University . " +
				"		?Z rdf:type ub:Department . " +
				"		?X ub:memberOf ?Z . " +
				"		?Z ub:subOrganizationOf ?Y . " +
				"		?X ub:undergraduateDegreeFrom ?Y " +
				" }";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 3
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X " +
				" WHERE " +
				" { " +
				" 		?X rdf:type ub:Publication . " +
				"		?X ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0> " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 4
			count = 0L;
			queryString = 
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

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 5
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X " +
				" WHERE " +
				" { " +
				"		?X rdf:type ub:Person . " +
				"		?X ub:memberOf <http://www.Department0.University0.edu> " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 6
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X WHERE { ?X rdf:type ub:Student } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 7
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X ?Y " +
				" WHERE " +
				" { " +
				"		?X rdf:type ub:Student . " +
				"		?Y rdf:type ub:Course . " +
				"		?X ub:takesCourse ?Y . " +
				"		<http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 8
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X ?Y ?Z " +
				" WHERE " +
				" { " +
				" 		?X rdf:type ub:Student . " + 
				"		?Y rdf:type ub:Department . " +
				"		?X ub:memberOf ?Y . " +
				"		?Y ub:subOrganizationOf <http://www.University0.edu> . " +
				"		?X ub:emailAddress ?Z " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 9
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X ?Y ?Z " +
				" WHERE " +
				" { " +
				" 		?X rdf:type ub:Student . " +
				"		?Y rdf:type ub:Faculty . " +
				"		?Z rdf:type ub:Course . " +
				"		?X ub:advisor ?Y . " +
				"		?Y ub:teacherOf ?Z . " +
				"		?X ub:takesCourse ?Z " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 10
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X " +
				" WHERE " +
				" { " +
				" 		?X rdf:type ub:Student . " +
				"		?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 11
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X " +
				" WHERE " +
				" { " +
				"		?X rdf:type ub:ResearchGroup . " +
				"		?X ub:subOrganizationOf <http://www.University0.edu> " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 12
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X ?Y " +
				" WHERE " +
				" { " +
				" 		?X rdf:type ub:Chair . " +
				"		?Y rdf:type ub:Department . " +
				"		?X ub:worksFor ?Y . " +
				"		?Y ub:subOrganizationOf <http://www.University0.edu> " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.next(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 13
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X " +
				" WHERE " +
				" { " +
				"		?X rdf:type ub:Person . " +
				"		<http://www.University0.edu> ub:hasAlumnus ?X " +
				" } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);

			//Query 14
			count = 0L;
			queryString = 
				" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				" PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
				" SELECT ?X " +
				" WHERE { ?X rdf:type ub:UndergraduateStudent } ";

			query = QueryFactory.create(queryString);
			qexec = QueryExecutionFactory.create(query, m);
			rs = qexec.execSelect();
			while( rs.hasNext() )
			{ count++; rs.nextSolution(); }
			qexec.close();
			System.out.println("count = " + count);
		}
		catch(Exception e) { e.printStackTrace(); }
	}
}
