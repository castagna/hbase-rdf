package com.talis.hbase.rdf.store;

import org.openjena.atlas.lib.Closeable;
import org.openjena.atlas.lib.Sync;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.Reorderable;

public interface GraphHBase extends Graph, Closeable, Sync, Reorderable 
{

}
