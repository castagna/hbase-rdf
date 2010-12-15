package com.talis.hbase.rdf;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.talis.hbase.rdf.sys.HBaseRdfMaker;

public class HBaseRdfFactory 
{
    public static Model createModel()
    { return ModelFactory.createModelForGraph( createGraph() ); }
    
    public static Graph createGraph() { return HBaseRdfMaker._createGraph() ; }
}