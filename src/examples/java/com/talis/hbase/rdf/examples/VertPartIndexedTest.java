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

package com.talis.hbase.rdf.examples;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.VCARD;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

/**
 * A test class for the HBase Jena model for the indexed layout.
 */
public class VertPartIndexedTest 
{
	public static void main( String[] args )
	{
		Store store = HBaseRdfFactory.connectStore( "Store/hbaserdf-vp-indexed.ttl" ) ;
		store.getTableFormatter().format() ;
		
		Model model = HBaseRdfFactory.connectDefaultModel( store ) ;
		
		model.add( model.createResource( "http://example.org/person#John" ), VCARD.FN, 
				   model.asRDFNode( Node.createLiteral( "John Smith" ) ) );
		model.add( model.createResource( "http://example.org/person#John" ), VCARD.EMAIL,
				   model.asRDFNode( Node.createLiteral( "jxs012000@utdallas.edu" ) ) );
		model.add( model.createResource( "http://example.org/person#Jim" ), VCARD.FN, 
				   model.asRDFNode( Node.createLiteral( "Jim Mason" ) ) );
		model.add( model.createResource( "http://example.org/person#Jim" ), VCARD.EMAIL,
				   model.asRDFNode( Node.createLiteral( "jxm012000@utdallas.edu" ) ) );
		model.add( model.createResource( "http://example.org/person#Bob" ), VCARD.FN, 
				   model.asRDFNode( Node.createLiteral( "Bob Brown" ) ) );
		model.add( model.createResource( "http://example.org/person#Bob" ), VCARD.EMAIL,
				   model.asRDFNode( Node.createLiteral( "bxb012000@utdallas.edu" ) ) );
		
		StmtIterator iter = model.listStatements();
		while( iter.hasNext() ) { System.out.println( iter.next().toString() ); }		
	
		iter = model.getResource( "http://example.org/person#John" ).listProperties();
		while( iter.hasNext() ) { System.out.println( iter.next().toString() ); }
		
		ResIterator resIter = model.listSubjects();
		while( resIter.hasNext() ) { System.out.println( resIter.next().toString() ); }
		
		String query = 
		" PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " +
		" SELECT ?x " +
		" WHERE " +
		" { " +
		" 		<http://example.org/person#John> vcard:FN ?x " +
		" } ";
		
		QueryExecution qe = QueryExecutionFactory.create( query, model );
		ResultSet rs = qe.execSelect();
		ResultSetFormatter.out( rs );
	}
}