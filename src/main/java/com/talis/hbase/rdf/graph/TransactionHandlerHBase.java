package com.talis.hbase.rdf.graph;

import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.impl.TransactionHandlerBase;
import com.talis.hbase.rdf.store.GraphHBase;

public class TransactionHandlerHBase extends TransactionHandlerBase implements TransactionHandler 
{

        private final GraphHBase graph ;

        public TransactionHandlerHBase (GraphHBase graph) {
                this.graph = graph;
        }

        @Override
        public void abort() {
                throw new UnsupportedOperationException("HBase RDF: 'abort' of a transaction not supported") ;
        }

        @Override
        public void begin() {}

        @Override
        public void commit() {
                graph.sync(true);
        }

        @Override
        public boolean transactionsSupported() {
                return false;
        }

}
