package com.talis.hbase.rdf.graph;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.impl.SimpleBulkUpdateHandler;
import com.talis.hbase.rdf.store.GraphHBaseBase;
import com.talis.hbase.rdf.store.GraphHBase;

public class BulkUpdateHandlerHBase  extends SimpleBulkUpdateHandler implements BulkUpdateHandler {

        GraphHBase graphHBase;

        public BulkUpdateHandlerHBase(GraphHBaseBase graph) {
                super(graph);
                this.graphHBase = graph;
        }

}
