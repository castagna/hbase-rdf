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

package com.talis.hbase.rdf.test.update;

import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
//import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.test.NodeCreateUtils;
//import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.Quad;
//import com.hp.hpl.jena.vocabulary.RDF;
//import com.hp.hpl.jena.rdf.model.Model;
//import com.hp.hpl.jena.vocabulary.RDF;
//import com.talis.hbase.rdf.HBaseRdfFactory;
//import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.layout.TableDescLayouts;
import com.talis.hbase.rdf.store.StoreLoaderPlus;
public abstract class TestStoreUpdateBase 
{
	Store store;
	StoreLoaderPlus loader;
	TableDescLayouts nodeT;
	
	abstract Store getStore();

	protected Node node( String str ) { return NodeCreateUtils.create( str ) ; }

	@Before public void init() 
	{
		this.store = getStore();
		this.loader = (StoreLoaderPlus) store.getLoader();
		this.nodeT = store.getTablesDesc() ;
	}
	
	protected long size( Node n ) { return store.getSize( n ) ; }
	
	@Test public void loadOneRemoveOne()
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "B" ), node( "B" ), node( "C" ) ) ;
		loader.finishBulkUpdate() ;
		assertEquals( "Added one triple", 1, size( Quad.defaultGraphNodeGenerated ) ) ;
		loader.startBulkUpdate() ;
		loader.deleteTuple( desc, node( "B" ), node( "B" ), node( "C" ) ) ;
		loader.finishBulkUpdate() ;
		assertEquals( "Back to the start", 0, size( Quad.defaultGraphNodeGenerated ) ) ;
	}
	
	@Test public void loadOneRemoveOneQ()
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "B" ), node( "B" ), node( "C" ), node( "D" ) ) ;
		loader.finishBulkUpdate() ;
		assertEquals( "Added one triple", 1, size( node( "B" ) ) ) ;
		loader.startBulkUpdate() ;
		loader.deleteTuple( desc, node( "B" ), node( "B" ), node( "C" ), node( "D" ) ) ;
		loader.finishBulkUpdate() ;
		assertEquals( "Back to the start", 0, size( node( "B" ) ) ) ;
	}
	
	@Test public void dupeSuppressed()
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "F" ), node( "A" ), node( "F" ) ) ;
		loader.addTuple( desc, node( "F" ), node( "A" ), node( "F" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Store added only one item", 1, size( Quad.defaultGraphNodeGenerated ) ) ;
		
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "G" ), node( "A" ), node( "F" ) ) ;
		loader.finishBulkUpdate() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "G" ), node( "A" ), node( "F" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Store added only one item", 2, size( Quad.defaultGraphNodeGenerated ) ) ;
	}
	
	@Test public void dupeSuppressedQ()
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "F" ), node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.addTuple( desc, node( "F" ), node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Store added only one item", 1, size( node( "F" ) ) ) ;
		
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "G" ), node( "A" ), node( "F" ), node( "K" ) ) ;
		loader.finishBulkUpdate() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "G" ), node( "A" ), node( "F" ), node( "K" ) ) ;
		loader.finishBulkUpdate() ;
		
		//Each named graph has a different set of HTables unlike the SDB Quad architecture 
		assertEquals( "Store added only one item", 2, size( node( "F" ) ) + size( node( "G" ) ) ) ;
	}
	
	@Test public void mixItUp()
	{
		TableDescLayouts desc1 = store.getTablesDesc() ;
		TableDescLayouts desc2 = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc2, node( "F" ), node( "A" ), node( "F" ), node( "G" ) ) ; 
		loader.addTuple( desc1, node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Store added to triples", 1, size( Quad.defaultGraphNodeGenerated ) ) ;
		assertEquals( "Store added to quads", 1, size( node( "F" ) ) ) ;
		
		loader.startBulkUpdate() ;
		loader.addTuple( desc2, node( "G" ), node( "A" ), node( "F" ), node( "K" ) ) ;
		loader.finishBulkUpdate() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc1, node( "A" ), node( "F" ), node( "K" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Store added one to triples", 2, size( Quad.defaultGraphNodeGenerated ) ) ;
		assertEquals( "Store added one to quads", 2, size( node( "F" ) ) + size( node( "G" ) ) ) ;
		
		loader.startBulkUpdate() ;
		loader.addTuple( desc2, node( "G" ), node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.deleteTuple( desc1, node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.addTuple( desc1, node( "B" ), node( "F" ), node( "G" ) ) ;
		loader.deleteTuple( desc2, node( "G" ), node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.deleteTuple( desc2, node( "B" ), node( "A" ), node( "F" ), node( "G" ) ) ;
		loader.addTuple( desc1, node( "B" ), node( "B" ), node( "B" ) ) ;
		loader.addTuple( desc2, node( "B" ), node( "B" ), node( "B" ), node( "B" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Store triple size correct", 3, size( Quad.defaultGraphNodeGenerated ) ) ;
		assertEquals( "Store quad size correct", 3, size( node( "G" ) ) + size( node( "B" ) ) + size( node( "F" ) ) ) ;
	}
	
	@Test( expected = IllegalArgumentException.class ) public void arityViolation()
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "One" ) ) ;
		loader.finishBulkUpdate() ;
	}
	
	@Test public void sizes() 
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "A" ), node( "A" ), node( "A" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "B" ), node( "B" ) ) ;
		loader.finishBulkUpdate() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "A" ), node( "A" ), node( "A" ), node( "A" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "A" ), node( "A" ), node( "A" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "B" ), node( "B" ), node( "B" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "C" ), node( "C" ), node( "C" ) ) ;
		loader.finishBulkUpdate() ;
		
		assertEquals( "Triple size right", 2l, store.getSize() ) ;
		assertEquals( "Quad size right", 1l, store.getSize( node( "A" ) ) ) ;
		assertEquals( "Quad size (2) right", 3l, store.getSize( node( "B" ) ) ) ;
	}
	
	//Transactions not implemented
/*	@Test public void rollback() 
	{
		Model model = HBaseRdfFactory.connectDefaultModel( store ) ;
		
		assertTrue( "Initially empty", model.isEmpty() ) ;
		model.begin() ;
		model.add( RDF.type, RDF.type, RDF.type ) ;
		assertTrue( "Uncommited triple can be seen", model.contains( RDF.type, RDF.type, RDF.type ) ) ;
		model.abort() ;
		assertTrue( "Nothing was added, the add aborted", model.isEmpty() ) ;
		model.add( RDF.type, RDF.type, RDF.type ) ;
		assertEquals( "Model contains 1 triple", 1l, model.size() ) ;
		model.begin() ; 
		model.remove( RDF.type, RDF.type, RDF.type ) ;
		model.abort() ;
		assertEquals( "Model still contains 1 triple", 1l, model.size() ) ;
	}
*/	
	@Test public void safeRemoveAll() 
	{
		TableDescLayouts desc = store.getTablesDesc() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "A" ), node( "A" ), node( "A" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "B" ), node( "B" ) ) ;
		loader.finishBulkUpdate() ;
		loader.startBulkUpdate() ;
		loader.addTuple( desc, node( "A" ), node( "A" ), node( "A" ), node( "A" ) ) ;
		loader.addTuple( desc, node( "A" ), node( "A" ), node( "B" ), node( "A" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "B" ), node( "B" ), node( "B" ) ) ;
		loader.addTuple( desc, node( "B" ), node( "C" ), node( "C" ), node( "C" ) ) ;
		loader.finishBulkUpdate() ;
		
		loader.startBulkUpdate() ;
		loader.deleteAll() ;
		loader.finishBulkUpdate() ;
		assertEquals( "Triples all removed", 0l, store.getSize() ) ;
		
		loader.startBulkUpdate() ;
		loader.deleteAll( node( "A" ) ) ;
		loader.finishBulkUpdate() ;
		assertEquals( "Quad A all removed", 0l, store.getSize( node( "A" ) ) ) ;
		assertEquals( "Quad B unaffected", 2l, store.getSize( node( "B" ) ) ) ;
	}
}