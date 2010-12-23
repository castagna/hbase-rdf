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

package com.talis.hbase.rdf.store;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.openjena.atlas.lib.Closeable;
import org.openjena.atlas.lib.Sync;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.update.GraphStore;

public class DatasetGraphHBase extends DatasetGraphCaching implements DatasetGraph, Sync, Closeable, GraphStore {

	private GraphHBase effectiveDefaultGraph ;
	private ReificationStyle style ;
	private Configuration configuration ;
	
	public DatasetGraphHBase ( Configuration configuration ) {
		this ( configuration, ReificationStyle.Standard ) ;
	}
	
	public DatasetGraphHBase( Configuration configuration, ReificationStyle style ) {
		this.configuration = configuration ;
		this.style = style; 
		this.effectiveDefaultGraph = getDefaultGraphHBase(); 
	}
	
	public GraphHBase getDefaultGraphHBase()
	{ return ( GraphHBase )getDefaultGraph(); }
	
	public void setEffectiveDefaultGraph( GraphHBase g ) { effectiveDefaultGraph = g ; }

    public GraphHBase getEffectiveDefaultGraph()  { return effectiveDefaultGraph ; }
    
	@Override
	protected void _close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean _containsGraph(Node graphNode) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected Graph _createDefaultGraph() { 
		return new GraphHBaseBase( this, Node.createURI("test"), style ); // I don't like this "test" around -- PC 
	}

	@Override
	protected Graph _createNamedGraph(Node graphNode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void addToDftGraph(Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void deleteFromDftGraph(Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void sync() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sync(boolean force) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void finishRequest() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startRequest() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Dataset toDataset() {
		// TODO Auto-generated method stub
		return null;
	}

	public Configuration getConfiguration() {
		return this.configuration;
	}

}